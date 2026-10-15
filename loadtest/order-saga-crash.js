// 장애 주입 부하 테스트: order-saga.js와 같은 주문을 넣는 동안 앱이나 RabbitMQ를 밖에서 강제로 죽였다 살리고,
// 모든 주문이 끝난 뒤 장부(재고·포인트)가 맞는지 판정한다. 장애 주입은 이 스크립트가 아니라 loadtest/crash-run.sh가 한다.
//
// order-saga.js와 다른 점
//   - 주문 요청이 연결 실패(status 0)나 5xx로 끝나면 실제 클라이언트처럼 같은 Idempotency-Key로 다시 보낸다
//     (RETRY_INTERVAL_MS 간격, 첫 요청부터 RETRY_MAX초까지). 재시도 횟수를 첫 실패 종류별로 센다
//   - 끝내 202를 못 받은 주문은 "접수 실패"로 따로 센다. 4xx는 재시도하지 않고 거절로 센다
//   - 재시도 끝에 202를 받은 주문은 콘솔에 RETRIED_ACCEPT 줄로 남긴다. crash-run.sh가 이 주문이
//     앱이 죽기 전에 이미 저장돼 있었는지(응답만 잃어버린 요청인지) DB로 확인한다
//   - 202를 받은 주문은 모두 콘솔에 ACCEPTED 줄로 남긴다. 최종 판정(①②)은 crash-tool.mjs가 이 줄과 DB로 한다.
//     이 스크립트의 settle 판정은 VU 메모리에 기대므로 참고용으로 결과 JSON의 k6Checks에 남는다
//   - 재시도로 VU가 오래 묶이므로 VU를 넉넉히 잡는다
//   - setup이 끝나면 LOAD_START 줄을 콘솔에 남긴다. crash-run.sh가 이 시각을 기준으로 장애를 넣는다
//
// 실행: loadtest/crash-run.sh가 실행한다. 직접 돌릴 때는
//   k6 run -e RATE=20 -e DURATION=90 -e MODE=app -e RUN=1 loadtest/order-saga-crash.js
//
// -e 옵션 (order-saga.js와 같은 것은 생략)
//   MODE              : 결과 파일 이름에 쓰는 장애 종류 (app | mq). 결과는 loadtest/results/crash-{MODE}-run{RUN}.json
//   DURATION          : 기본값 90
//   DRAIN_MAX         : 기본값 300
//   RETRY_MAX         : 같은 키로 재시도하는 최대 시간(초) (기본값 60)
//   RETRY_INTERVAL_MS : 재시도 간격 (기본값 500)
//   VUS               : 기본값 RATE × 3 + RATE × 40 (죽은 동안 재시도로 묶이는 VU 몫)

import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Gauge, Trend } from 'k6/metrics';

const RATE = parseInt(__ENV.RATE, 10);
const RUN = __ENV.RUN || '1';
const MODE = __ENV.MODE || 'app';
const DURATION = parseInt(__ENV.DURATION || '90', 10);
const DRAIN_MAX = parseInt(__ENV.DRAIN_MAX || '300', 10);
const RETRY_MAX = parseInt(__ENV.RETRY_MAX || '60', 10);
const RETRY_INTERVAL_MS = parseInt(__ENV.RETRY_INTERVAL_MS || '500', 10);
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const PROMETHEUS_URL = __ENV.PROMETHEUS_URL || 'http://localhost:9090';
const GRAFANA_URL = __ENV.GRAFANA_URL || 'http://localhost:3000';

if (!(RATE > 0)) {
	throw new Error(`RATE는 1 이상의 정수여야 합니다. 입력: ${__ENV.RATE}`);
}

// orders가 끝나고 settle이 시작되기 전 간격. 이 안에 끝나지 않은 주문 요청은 끊겨서 정산에서 빠진다
// (시작한 주문 − 접수 − 거절 − 접수 실패 = 끊긴 주문으로 드러남)
const GRACEFUL_STOP_SECONDS = 5;
// 평소 몫(RATE × 3)에, 앱이 죽은 동안 재시도로 묶이는 VU 몫(최대 40초 분량)을 더한다. 모자라면 dropped_iterations로 드러난다
const VUS = parseInt(__ENV.VUS || String(Math.max(10, RATE * 3) + RATE * 40), 10);

// 주문 구성: 포인트 부족 15%, 품절 상품 15%, 나머지 70%는 성공해야 하는 주문
const POOR_RATIO = 0.15;
const SCARCE_RATIO = 0.15;

const RICH_USERS = 30;
const POOR_USERS = 6;
const RICH_POINT = 100000000;
// 가장 싼 상품(3000)보다 적어서 이 회원의 주문은 모두 포인트 부족으로 취소된다
const POOR_POINT = 1000;
const PLENTY_PRODUCTS = 5;
const PLENTY_STOCK = 1000000;
const SCARCE_STOCK = 5;

const PRODUCT_COUNT = PLENTY_PRODUCTS + 1;
const SCARCE_INDEX = PLENTY_PRODUCTS;
const USER_COUNT = RICH_USERS + POOR_USERS;
const KINDS = ['success', 'out_of_stock', 'payment_failed'];
const RESULTS = ['COMPLETED', 'OUT_OF_STOCK', 'PAYMENT_FAILED', 'UNFINISHED'];

const JSON_HEADERS = { 'Content-Type': 'application/json' };

// 실패 종류. refused: 연결 자체가 거부됨(앱이 떠 있지 않음, 요청이 서버에 닿지 않음)
// broken: 연결 후 응답 전에 끊김(요청이 서버에 닿았을 수 있음), 5xx: 서버가 오류로 응답
const FAILURE_KINDS = ['refused', 'broken', '5xx'];

// --- 주문 단계 지표 (주문 요청에서만 기록하므로 setup/settle 요청은 섞이지 않는다)
const orderStarted = new Counter('order_started');
const orderAccepted = new Counter('order_accepted');
const orderRejected = new Counter('order_rejected');
const orderAcceptFailed = new Counter('order_accept_failed');
// 재시도 없이 202를 받은 요청의 응답 시간
const orderDuration = new Trend('order_create_duration', true);
// 재시도 요청 수(첫 요청 제외)와 재시도한 주문 수. 태그 first = 그 주문의 첫 실패 종류
const orderRetryRequests = new Counter('order_retry_requests');
const retriedOrders = new Counter('retried_orders');
const retriedAccepted = new Counter('retried_accepted');
// 재시도 끝에 접수된 주문의 첫 요청부터 202까지 걸린 시간
const retryWaitMs = new Trend('retry_wait_ms', true);

// --- settle 단계 지표. 태그별 합계는 handleSummary에서 읽을 수 있도록 options.thresholds에 등록한다
const settledOrders = new Counter('settled_orders');
const finalResult = new Counter('final_result');
const completedQuantity = new Counter('completed_quantity');
const completedAmount = new Counter('completed_amount');
const drainMs = new Gauge('drain_ms');
const inProgressAtDeadline = new Gauge('in_progress_at_deadline');
const loadEndAt = new Gauge('load_end_at');
const drainEndAt = new Gauge('drain_end_at');

// --- teardown 지표
const remainingStock = new Gauge('remaining_stock');
const remainingPoint = new Gauge('remaining_point');
const dlqMessages = new Gauge('dlq_messages');
const promValue = new Gauge('prom');
const teardownAt = new Gauge('teardown_at');

const PROM_QUERIES = {
	relayPublishPerSecMax: (w) => `max_over_time(sum(rate(mega_outbox_relay_published_total[20s]))[${w}s:5s])`,
	outboxReadyMax: (w) => `max_over_time(mega_outbox_ready[${w}s])`,
	queueWaitingTotalMax: (w) => `max_over_time(sum(rabbitmq_detailed_queue_messages{queue!="mega.saga.dlq"})[${w}s:5s])`,
	sagaDurationP95Seconds: (w) => `histogram_quantile(0.95, sum by (le) (increase(mega_order_saga_duration_seconds_bucket[${w}s])))`,
	postOrdersServerP95Seconds: (w) => `histogram_quantile(0.95, sum by (le) (increase(http_server_requests_seconds_bucket{method="POST", uri="/api/v1/orders"}[${w}s])))`,
	hikariPendingMax: (w) => `max_over_time(sum(hikaricp_connections_pending)[${w}s:5s])`,
	hikariActiveMax: (w) => `max_over_time(sum(hikaricp_connections_active)[${w}s:5s])`,
	finishPerSecMax: (w) => `max_over_time((sum(rate(mega_orders_completed_total[20s])) + sum(rate(mega_orders_canceled_total[20s])))[${w}s:5s])`,
	// Relay 배치 지표 (배치 방식 Relay에만 있음. 없으면 null)
	relayBatchSizeAvg: (w) => `sum(increase(mega_outbox_relay_batch_size_sum[${w}s])) / sum(increase(mega_outbox_relay_batch_size_count[${w}s]))`,
	relayBatchDurationAvgSeconds: (w) => `sum(increase(mega_outbox_relay_batch_duration_seconds_sum[${w}s])) / sum(increase(mega_outbox_relay_batch_duration_seconds_count[${w}s]))`,
	relayBatchDurationP95Seconds: (w) => `histogram_quantile(0.95, sum by (le) (increase(mega_outbox_relay_batch_duration_seconds_bucket[${w}s])))`,
	// 앱 프로세스와 기기 전체 CPU 사용률(0~1). 앱·k6·Docker가 같은 기기에서 돌므로 기기 전체 값도 본다
	// 멱등 컨슈머가 중복으로 건너뛴 수 (단계별). 앱이 재시작되면 Counter가 0부터 다시 시작하지만 increase()가 이어 붙인다.
	// 다만 앱이 죽기 직전 마지막 스크레이프 이후에 오른 값은 Prometheus에 남지 않는다
	duplicateSkippedLookup: (w) => `sum(increase(mega_idempotency_duplicate_skipped_total{stage="lookup"}[${w}s]))`,
	duplicateSkippedUnique: (w) => `sum(increase(mega_idempotency_duplicate_skipped_total{stage="unique"}[${w}s]))`,
	relayFailedTotal: (w) => `sum(increase(mega_outbox_relay_failed_total[${w}s]))`,
	processCpuMax: (w) => `max_over_time(process_cpu_usage[${w}s])`,
	systemCpuMax: (w) => `max_over_time(system_cpu_usage[${w}s])`,
};

// 태그가 붙은 지표는 threshold로 등록해야 handleSummary에 태그별 값이 나온다 (조건은 항상 참)
function taggedThresholds() {
	const thresholds = {};
	for (let p = 0; p < PRODUCT_COUNT; p++) {
		thresholds[`completed_quantity{product:${p}}`] = ['count>=0'];
		thresholds[`remaining_stock{product:${p}}`] = ['value>=0'];
	}
	for (let u = 0; u < USER_COUNT; u++) {
		thresholds[`completed_amount{user:${u}}`] = ['count>=0'];
		thresholds[`remaining_point{user:${u}}`] = ['value>=0'];
	}
	for (const kind of KINDS) {
		for (const result of RESULTS) {
			thresholds[`final_result{kind:${kind},result:${result}}`] = ['count>=0'];
		}
	}
	for (const kind of FAILURE_KINDS) {
		thresholds[`retried_orders{first:${kind}}`] = ['count>=0'];
		thresholds[`retried_accepted{first:${kind}}`] = ['count>=0'];
		thresholds[`order_accept_failed{last:${kind}}`] = ['count>=0'];
	}
	for (const key of Object.keys(PROM_QUERIES)) {
		thresholds[`prom{query:${key}}`] = ['value>=0'];
	}
	return thresholds;
}

export const options = {
	scenarios: {
		orders: {
			executor: 'constant-arrival-rate',
			rate: RATE,
			timeUnit: '1s',
			duration: `${DURATION}s`,
			preAllocatedVUs: VUS,
			maxVUs: VUS,
			gracefulStop: `${GRACEFUL_STOP_SECONDS}s`,
			exec: 'placeOrder',
		},
		settle: {
			executor: 'per-vu-iterations',
			vus: VUS,
			iterations: 1,
			startTime: `${DURATION + GRACEFUL_STOP_SECONDS}s`,
			maxDuration: `${DRAIN_MAX + 120}s`,
			exec: 'settle',
		},
	},
	thresholds: taggedThresholds(),
	summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
	setupTimeout: '120s',
	teardownTimeout: '120s',
};

// 이 VU가 접수시킨 주문 (settle 단계에서 이 VU가 정산한다)
const myOrders = [];

export function setup() {
	const startedAt = Date.now();
	const inProgress = fetchInProgressOrders();
	if (inProgress !== 0) {
		throw new Error(`시작 전 진행 중 주문이 ${inProgress}건 있습니다. drain 측정이 섞이므로 정리 후 다시 실행하세요.`);
	}

	const runTag = `crash-${MODE}-r${RATE}-${RUN}-${startedAt}`;
	const products = [];
	for (let p = 0; p < PRODUCT_COUNT; p++) {
		const scarce = p === SCARCE_INDEX;
		const price = scarce ? 4000 : 3000 + p * 500;
		const stock = scarce ? SCARCE_STOCK : PLENTY_STOCK;
		const res = postJson('/api/v1/products', { name: `${runTag}-p${p}`, price, stockQuantity: stock }, 'setup_create_product');
		products.push({ index: p, id: res.json('id'), price, initialStock: stock, scarce });
	}

	const users = [];
	for (let u = 0; u < USER_COUNT; u++) {
		const poor = u >= RICH_USERS;
		const point = poor ? POOR_POINT : RICH_POINT;
		const id = postJson('/api/v1/users', { name: `${runTag}-u${u}` }, 'setup_create_user').json('id');
		postJson(`/api/v1/users/${id}/points`, { amount: point }, 'setup_charge_point', 200);
		users.push({ index: u, id, initialPoint: point, poor });
	}

	const loadStart = Date.now();
	console.log(`LOAD_START ${loadStart} ${runTag}`);
	return { startedAt, loadStart, runTag, products, users };
}

export function placeOrder(data) {
	const order = pickOrder(data);
	const key = crypto.randomUUID();
	const body = JSON.stringify({ userId: order.userId, items: [{ productId: order.productId, quantity: order.quantity }] });
	const params = { headers: { ...JSON_HEADERS, 'Idempotency-Key': key }, tags: { name: 'create_order' } };
	orderStarted.add(1);

	const firstAt = Date.now();
	let res = http.post(`${BASE_URL}/api/v1/orders`, body, params);
	let attempts = 1;
	let firstFailure = null;
	let firstError = null;
	while (isRetryable(res)) {
		if (firstFailure === null) {
			firstFailure = failureKind(res);
			firstError = res.error || `status ${res.status}`;
		}
		if (Date.now() + RETRY_INTERVAL_MS > firstAt + RETRY_MAX * 1000) {
			break;
		}
		sleep(RETRY_INTERVAL_MS / 1000);
		res = http.post(`${BASE_URL}/api/v1/orders`, body, { ...params, tags: { name: 'create_order_retry' } });
		attempts++;
		orderRetryRequests.add(1);
	}

	if (firstFailure !== null) {
		retriedOrders.add(1, { first: firstFailure });
	}
	if (res.status === 202 && attempts === 1) {
		orderDuration.add(res.timings.duration);
	}
	if (res.status !== 202) {
		if (isRetryable(res)) {
			orderAcceptFailed.add(1, { last: failureKind(res) });
		} else {
			orderRejected.add(1, { status: String(res.status) });
		}
		return;
	}

	orderAccepted.add(1);
	const orderId = res.json('orderId');
	myOrders.push({ ...order, orderId });
	// 장부 판정은 crash-tool.mjs가 이 줄과 DB로 다시 한다. 부하 종료 때 재시도 중이던 iteration이 끊기면
	// 그 VU의 메모리(myOrders)가 사라져 settle이 앞서 접수된 주문까지 놓칠 수 있어서다 (k6 v2.2.0에서 확인)
	console.log(`ACCEPTED ${JSON.stringify({ orderId, key, kind: order.kind, p: order.productIndex, u: order.userIndex, q: order.quantity, a: order.amount })}`);
	if (firstFailure !== null) {
		retriedAccepted.add(1, { first: firstFailure });
		retryWaitMs.add(Date.now() - firstAt);
		console.log(`RETRIED_ACCEPT ${JSON.stringify({ key, orderId, attempts, firstAt, firstFailure, firstError, acceptedAt: Date.now() })}`);
	}
}

function isRetryable(res) {
	return res.status === 0 || res.status >= 500;
}

function failureKind(res) {
	if (res.status !== 0) {
		return '5xx';
	}
	return /refused/i.test(res.error || '') ? 'refused' : 'broken';
}

function pickOrder(data) {
	const r = Math.random();
	if (r < POOR_RATIO) {
		const user = data.users[RICH_USERS + randomInt(POOR_USERS)];
		const product = data.products[randomInt(PLENTY_PRODUCTS)];
		return toOrder('payment_failed', user, product, 1);
	}
	const user = data.users[randomInt(RICH_USERS)];
	if (r < POOR_RATIO + SCARCE_RATIO) {
		return toOrder('out_of_stock', user, data.products[SCARCE_INDEX], 1);
	}
	return toOrder('success', user, data.products[randomInt(PLENTY_PRODUCTS)], 1 + randomInt(3));
}

function toOrder(kind, user, product, quantity) {
	return {
		kind,
		userIndex: user.index,
		userId: user.id,
		productIndex: product.index,
		productId: product.id,
		quantity,
		amount: product.price * quantity,
	};
}

export function settle() {
	const loadEnd = exec.scenario.startTime - GRACEFUL_STOP_SECONDS * 1000;
	const deadline = loadEnd + DRAIN_MAX * 1000;
	// 한 VU만 앱에 1초 간격으로 물어 drain 시간을 잰다. 나머지 VU는 앱에 부담을 주지 않도록 Prometheus를 본다
	const leader = exec.scenario.iterationInTest === 0;

	let inProgress = null;
	while (Date.now() < deadline) {
		if (!leader && myOrders.length === 0) {
			break;
		}
		inProgress = leader ? fetchInProgressOrders() : fetchInProgressOrdersFromPrometheus();
		if (inProgress === 0) {
			break;
		}
		// VU가 많으므로 나머지 VU는 Prometheus에 주는 부담을 줄이려고 더 드물게 본다
		sleep(leader ? 1 : 5);
	}
	if (leader) {
		loadEndAt.add(loadEnd);
		if (inProgress === 0) {
			drainEndAt.add(Date.now());
			drainMs.add(Date.now() - loadEnd);
		} else {
			drainMs.add(-1);
			inProgressAtDeadline.add(inProgress === null ? -1 : inProgress);
		}
	}

	for (const order of myOrders) {
		const res = http.get(`${BASE_URL}/api/v1/orders/${order.orderId}/status`, { tags: { name: 'settle_get_status' } });
		settledOrders.add(1);
		const result = toResult(res);
		finalResult.add(1, { kind: order.kind, result });
		if (result === 'COMPLETED') {
			completedQuantity.add(order.quantity, { product: String(order.productIndex) });
			completedAmount.add(order.amount, { user: String(order.userIndex) });
		}
	}
}

function toResult(res) {
	if (res.status !== 200) {
		return 'UNFINISHED';
	}
	const status = res.json('status');
	if (status === 'COMPLETED') {
		return 'COMPLETED';
	}
	if (status === 'CANCELED') {
		return res.json('cancelReason');
	}
	return 'UNFINISHED';
}

export function teardown(data) {
	// 마지막 상태가 Prometheus에 들어오도록 기다린다 (앱 스크레이프 5초 + RabbitMQ 큐 통계 갱신 5초)
	sleep(12);
	const now = Date.now();

	for (const product of data.products) {
		const res = http.get(`${BASE_URL}/api/v1/products/${product.id}`, { tags: { name: 'teardown_get_product' } });
		if (res.status === 200) {
			remainingStock.add(res.json('stockQuantity'), { product: String(product.index) });
		}
	}
	for (const user of data.users) {
		const res = http.get(`${BASE_URL}/api/v1/users/${user.id}/points`, { tags: { name: 'teardown_get_point' } });
		if (res.status === 200) {
			remainingPoint.add(Number(res.json('pointBalance')), { user: String(user.index) });
		}
	}

	const dlq = queryPrometheus('sum(rabbitmq_detailed_queue_messages{queue="mega.saga.dlq"})');
	if (dlq !== null) {
		dlqMessages.add(dlq);
	}

	const windowSeconds = Math.ceil((now - data.startedAt) / 1000);
	for (const [key, query] of Object.entries(PROM_QUERIES)) {
		const value = queryPrometheus(query(windowSeconds));
		if (value !== null) {
			promValue.add(value, { query: key });
		}
	}
	teardownAt.add(now);
}

// teardown은 HTTP 요청은 할 수 있지만 집계된 지표를 읽을 수 없고,
// handleSummary는 집계된 지표를 받지만 HTTP 요청을 할 수 없다.
// 그래서 settle·teardown이 값을 지표로 기록하고, 판정은 handleSummary에서 한다.
export function handleSummary(data) {
	const metric = (name) => data.metrics[name] && data.metrics[name].values;
	const count = (name) => (metric(name) ? metric(name).count : 0);
	const gauge = (name) => (metric(name) ? metric(name).value : null);
	const setupData = data.setup_data;

	const started = count('order_started');
	const accepted = count('order_accepted');
	const rejected = count('order_rejected');
	const acceptFailed = count('order_accept_failed');
	const byFailure = (name) => Object.fromEntries(FAILURE_KINDS.map((kind) => [kind, count(`${name}:${kind}}`)]));
	const dropped = count('dropped_iterations');
	const settled = count('settled_orders');
	const drain = gauge('drain_ms');
	const loadEnd = gauge('load_end_at');
	const drainEnd = gauge('drain_end_at');
	const teardownTime = gauge('teardown_at');
	const dlq = gauge('dlq_messages');

	const results = {};
	for (const kind of KINDS) {
		results[kind] = {};
		for (const result of RESULTS) {
			results[kind][result] = count(`final_result{kind:${kind},result:${result}}`);
		}
	}
	const resultTotal = (result) => KINDS.reduce((sum, kind) => sum + results[kind][result], 0);

	const products = setupData.products.map((p) => {
		const completed = count(`completed_quantity{product:${p.index}}`);
		const remaining = gauge(`remaining_stock{product:${p.index}}`);
		const expected = p.initialStock - completed;
		return { index: p.index, id: p.id, scarce: p.scarce, initialStock: p.initialStock, completedQuantity: completed, expected, remaining, pass: remaining === expected };
	});
	const users = setupData.users.map((u) => {
		const completed = count(`completed_amount{user:${u.index}}`);
		const remaining = gauge(`remaining_point{user:${u.index}}`);
		const expected = u.initialPoint - completed;
		return { index: u.index, id: u.id, poor: u.poor, initialPoint: u.initialPoint, completedAmount: completed, expected, remaining, pass: remaining === expected };
	});

	const unfinished = resultTotal('UNFINISHED');
	const finished = settled - unfinished;
	const drainSeconds = drain === null || drain < 0 ? null : drain / 1000;
	const checks = {
		trackedAllOrders: settled === accepted,
		stockLedger: products.every((p) => p.pass),
		pointLedger: users.every((u) => u.pass),
		noUnfinishedOrders: unfinished === 0 && drain !== null && drain >= 0,
		dlqEmpty: dlq === 0,
	};
	checks.passed = Object.values(checks).every(Boolean);

	const prom = {};
	for (const key of Object.keys(PROM_QUERIES)) {
		prom[key] = round(gauge(`prom{query:${key}}`), 3);
	}

	const trend = metric('order_create_duration') || {};
	const retryWait = metric('retry_wait_ms') || {};
	const summary = {
		mode: MODE,
		rate: RATE,
		run: RUN,
		durationSeconds: DURATION,
		vus: VUS,
		runTag: setupData.runTag,
		finishedAt: new Date().toISOString(),
		orders: {
			started,
			accepted,
			rejected,
			// 같은 키로 RETRY_MAX초 동안 재시도해도 202를 못 받은 주문 (last = 마지막 실패 종류)
			acceptFailed,
			acceptFailedByLast: byFailure('order_accept_failed{last'),
			// 부하 종료(gracefulStop) 때 재시도 중에 끊겨 결과가 없는 주문
			interrupted: started - accepted - rejected - acceptFailed,
			droppedIterations: dropped,
			acceptedPerSec: round(accepted / DURATION),
			settled,
			finished,
			unfinished,
			byResult: { COMPLETED: resultTotal('COMPLETED'), OUT_OF_STOCK: resultTotal('OUT_OF_STOCK'), PAYMENT_FAILED: resultTotal('PAYMENT_FAILED'), UNFINISHED: unfinished },
			byKind: results,
		},
		retry: {
			// 첫 요청을 뺀 재시도 요청 수
			requests: count('order_retry_requests'),
			// 한 번이라도 재시도한 주문 수 (first = 첫 실패 종류)
			orders: count('retried_orders'),
			ordersByFirst: byFailure('retried_orders{first'),
			accepted: count('retried_accepted'),
			acceptedByFirst: byFailure('retried_accepted{first'),
			// 재시도 끝에 접수된 주문의 첫 요청부터 202까지 걸린 시간
			waitMs: { avg: round(retryWait.avg), med: round(retryWait.med), max: round(retryWait.max) },
		},
		createLatencyMs: { avg: round(trend.avg), med: round(trend.med), p95: round(trend['p(95)']), p99: round(trend['p(99)']), max: round(trend.max) },
		drainSeconds: drainSeconds === null ? null : round(drainSeconds),
		inProgressAtDeadline: gauge('in_progress_at_deadline'),
		// 주문 시작부터 마지막 주문이 끝날 때까지의 평균 처리 속도
		finishedPerSec: drainSeconds === null ? null : round(finished / (DURATION + drainSeconds)),
		prometheus: prom,
		dlqMessages: dlq,
		checks,
		products,
		users,
		times: {
			loadStart: setupData.loadStart,
			loadEnd,
			drainEnd,
			teardown: teardownTime,
		},
		grafana: {
			from: new Date(setupData.startedAt).toISOString(),
			loadEnd: loadEnd === null ? null : new Date(loadEnd).toISOString(),
			to: teardownTime === null ? null : new Date(teardownTime).toISOString(),
			url: teardownTime === null ? null : `${GRAFANA_URL}/d/mega-order-saga?from=${setupData.startedAt}&to=${teardownTime}`,
		},
	};

	return {
		stdout: report(summary),
		[`loadtest/results/crash-${MODE}-run${RUN}.json`]: JSON.stringify(summary, null, 2),
	};
}

function report(s) {
	const mark = (v) => (v ? 'PASS' : 'FAIL');
	const failedProducts = s.products.filter((p) => !p.pass).map((p) => `p${p.index}(기대 ${p.expected}, 실제 ${p.remaining})`);
	const failedUsers = s.users.filter((u) => !u.pass).map((u) => `u${u.index}(기대 ${u.expected}, 실제 ${u.remaining})`);
	const r = s.orders.byResult;
	return `
==================== 주문 Saga 장애 주입 테스트 결과 ====================
${s.mode}, RATE ${s.rate}/s × ${s.durationSeconds}s, RUN ${s.run}, VU ${s.vus}

[주문]  시작 ${s.orders.started}, 접수 ${s.orders.accepted} (${s.orders.acceptedPerSec}/s), 거절 ${s.orders.rejected}, 접수 실패 ${s.orders.acceptFailed}, 끊김 ${s.orders.interrupted}, 못 보낸 요청(dropped) ${s.orders.droppedIterations}
  재시도: 주문 ${s.retry.orders}건 ${JSON.stringify(s.retry.ordersByFirst)}, 재시도 요청 ${s.retry.requests}회, 재시도 끝에 접수 ${s.retry.accepted}건 (대기 avg ${s.retry.waitMs.avg} / max ${s.retry.waitMs.max} ms)
  결과: COMPLETED ${r.COMPLETED} / OUT_OF_STOCK ${r.OUT_OF_STOCK} / PAYMENT_FAILED ${r.PAYMENT_FAILED} / 미완료 ${r.UNFINISHED}
  접수 응답(재시도 없이 202) avg / p95 / p99 / max : ${s.createLatencyMs.avg} / ${s.createLatencyMs.p95} / ${s.createLatencyMs.p99} / ${s.createLatencyMs.max} ms
  drain ${s.drainSeconds === null ? '시간 초과' : s.drainSeconds + 's'}, 끝난 속도 ${s.finishedPerSec}/s

[Prometheus]
${Object.entries(s.prometheus).map(([k, v]) => `  ${k.padEnd(28)}: ${v}`).join('\n')}

[판정]
  정산 주문 수 = 접수 수 (${s.orders.settled} / ${s.orders.accepted}) : ${mark(s.checks.trackedAllOrders)}
  ① 상품별 처음 재고 − 완료 수량 = 남은 재고 : ${mark(s.checks.stockLedger)}${failedProducts.length ? ' ' + failedProducts.join(', ') : ''}
  ② 회원별 처음 포인트 − 완료 금액 = 남은 포인트 : ${mark(s.checks.pointLedger)}${failedUsers.length ? ' ' + failedUsers.join(', ') : ''}
  ③ 미완료 주문 0건                          : ${mark(s.checks.noUnfinishedOrders)}
  ④ DLQ 0건 (${s.dlqMessages})                        : ${mark(s.checks.dlqEmpty)}
  종합                                       : ${mark(s.checks.passed)}

Grafana: ${s.grafana.url}
====================================================================
`;
}

function postJson(path, body, name, expectedStatus = 201) {
	const res = http.post(`${BASE_URL}${path}`, JSON.stringify(body), { headers: JSON_HEADERS, tags: { name } });
	if (res.status !== expectedStatus) {
		throw new Error(`${name} 실패: status=${res.status}, body=${res.body}`);
	}
	return res;
}

// 앱의 mega_orders_by_status Gauge에서 PENDING + STOCK_RESERVED 합계를 읽는다 (DB 전체 기준)
function fetchInProgressOrders() {
	const res = http.get(`${BASE_URL}/actuator/prometheus`, { tags: { name: 'poll_in_progress' } });
	if (res.status !== 200) {
		return null;
	}
	let total = 0;
	for (const line of res.body.split('\n')) {
		if (line.startsWith('mega_orders_by_status{') && /status="(PENDING|STOCK_RESERVED)"/.test(line)) {
			total += Number(line.substring(line.lastIndexOf(' ') + 1));
		}
	}
	return total;
}

function fetchInProgressOrdersFromPrometheus() {
	return queryPrometheus('sum(mega_orders_by_status{status=~"PENDING|STOCK_RESERVED"})');
}

function queryPrometheus(query) {
	const res = http.get(`${PROMETHEUS_URL}/api/v1/query?query=${encodeURIComponent(query)}`, { tags: { name: 'prometheus_query' } });
	if (res.status !== 200) {
		return null;
	}
	const result = res.json('data.result');
	if (!result || result.length === 0) {
		return null;
	}
	const value = Number(result[0].value[1]);
	return Number.isFinite(value) ? value : null;
}

function randomInt(max) {
	return Math.floor(Math.random() * max);
}

function round(v, digits = 2) {
	if (typeof v !== 'number' || !Number.isFinite(v)) {
		return null;
	}
	const scale = 10 ** digits;
	return Math.round(v * scale) / scale;
}
