// 주문 Saga 전체 흐름 부하 테스트: 정해진 속도로 주문을 넣고, 모두 끝날 때까지 기다린 뒤 장부(재고·포인트)를 판정한다
//
// 실행 (프로젝트 루트에서, 앱·Docker Compose가 떠 있는 상태):
//   k6 run -e RATE=10 -e RUN=1 loadtest/order-saga.js
//
// -e 옵션
//   RATE           : 초당 주문 수 (필수)
//   RUN            : 회차 번호. 결과 파일 이름에 쓴다 (기본값 1)
//   LABEL          : 비교 실험용 이름(예: before, after). 주면 결과 파일 이름에 붙는다 (기본값 없음)
//   DURATION       : 주문을 넣는 시간(초) (기본값 60)
//   DRAIN_MAX      : 부하가 끝난 뒤 진행 중 주문이 0이 되기를 기다리는 최대 시간(초) (기본값 180)
//   BASE_URL       : 기본값 http://localhost:8080
//   PROMETHEUS_URL : 기본값 http://localhost:9090
//   GRAFANA_URL    : 기본값 http://localhost:3000
//
// 결과 요약은 콘솔에 출력하고 loadtest/results/saga[-{LABEL}]-rate{RATE}-run{RUN}.json 으로 저장한다.
//
// 진행 순서
//   1. setup    : 이번 실행 전용 상품·회원을 만든다
//   2. orders   : constant-arrival-rate로 DURATION초 동안 주문. 각 VU는 자기가 넣은 주문을 메모리에 적어 둔다
//   3. settle   : 같은 VU들이 진행 중 주문이 0이 되기를 기다린 뒤(drain), 자기 주문의 최종 상태를 조회해
//                 상품별 완료 수량·회원별 완료 금액을 태그 지표로 기록한다
//   4. teardown : 남은 재고·포인트, DLQ, Prometheus 지표를 조회해 지표로 기록한다
//   5. handleSummary : 지표를 모아 판정하고 JSON으로 저장한다
//
// k6는 VU끼리 메모리를 공유하지 못하고 teardown은 VU의 데이터를 볼 수 없다. 그래서 주문을 넣은 VU가
// settle 단계에서 직접 정산한다. settle은 orders와 같은 수의 VU를 쓰고 시간이 겹치지 않으므로
// k6가 같은 VU들을 다시 쓴다. 혹시 다른 VU가 배정되면 "정산한 주문 수 ≠ 접수된 주문 수"로 드러나 판정이 FAIL이 된다.

import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Gauge, Trend } from 'k6/metrics';

const RATE = parseInt(__ENV.RATE, 10);
const RUN = __ENV.RUN || '1';
const LABEL = __ENV.LABEL || '';
const DURATION = parseInt(__ENV.DURATION || '60', 10);
const DRAIN_MAX = parseInt(__ENV.DRAIN_MAX || '180', 10);
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const PROMETHEUS_URL = __ENV.PROMETHEUS_URL || 'http://localhost:9090';
const GRAFANA_URL = __ENV.GRAFANA_URL || 'http://localhost:3000';

if (!(RATE > 0)) {
	throw new Error(`RATE는 1 이상의 정수여야 합니다. 입력: ${__ENV.RATE}`);
}

// orders가 끝나고 settle이 시작되기 전 간격. 이 안에 끝나지 않은 주문 요청은 끊겨서 정산에서 빠진다(판정에서 드러남)
const GRACEFUL_STOP_SECONDS = 3;
// 응답이 느려져도 도착률을 지킬 수 있게 넉넉히 잡는다. 모자라면 dropped_iterations로 드러난다
const VUS = Math.max(10, RATE * 3);

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

// --- 주문 단계 지표 (주문 요청에서만 기록하므로 setup/settle 요청은 섞이지 않는다)
const orderAccepted = new Counter('order_accepted');
const orderRejected = new Counter('order_rejected');
const orderDuration = new Trend('order_create_duration', true);

// --- settle 단계 지표. 태그별 합계는 handleSummary에서 읽을 수 있도록 options.thresholds에 등록한다
const settledOrders = new Counter('settled_orders');
const finalResult = new Counter('final_result');
const completedQuantity = new Counter('completed_quantity');
const completedAmount = new Counter('completed_amount');
const drainMs = new Gauge('drain_ms');
const inProgressAtDeadline = new Gauge('in_progress_at_deadline');
const loadEndAt = new Gauge('load_end_at');

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

	const runTag = `saga-r${RATE}-${RUN}-${startedAt}`;
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

	return { startedAt, runTag, products, users };
}

export function placeOrder(data) {
	const order = pickOrder(data);
	const res = http.post(
		`${BASE_URL}/api/v1/orders`,
		JSON.stringify({ userId: order.userId, items: [{ productId: order.productId, quantity: order.quantity }] }),
		{ headers: { ...JSON_HEADERS, 'Idempotency-Key': crypto.randomUUID() }, tags: { name: 'create_order' } },
	);

	if (res.status !== 0) {
		orderDuration.add(res.timings.duration);
	}
	if (res.status !== 202) {
		orderRejected.add(1, { status: String(res.status) });
		return;
	}
	orderAccepted.add(1);
	myOrders.push({ ...order, orderId: res.json('orderId') });
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
		inProgress = leader ? fetchInProgressOrders() : fetchInProgressOrdersFromPrometheus();
		if (inProgress === 0) {
			break;
		}
		sleep(leader ? 1 : 2);
	}
	if (leader) {
		loadEndAt.add(loadEnd);
		if (inProgress === 0) {
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

	const accepted = count('order_accepted');
	const rejected = count('order_rejected');
	const dropped = count('dropped_iterations');
	const settled = count('settled_orders');
	const drain = gauge('drain_ms');
	const loadEnd = gauge('load_end_at');
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
	const summary = {
		label: LABEL || null,
		rate: RATE,
		run: RUN,
		durationSeconds: DURATION,
		vus: VUS,
		runTag: setupData.runTag,
		finishedAt: new Date().toISOString(),
		orders: {
			accepted,
			rejected,
			droppedIterations: dropped,
			acceptedPerSec: round(accepted / DURATION),
			settled,
			finished,
			unfinished,
			byResult: { COMPLETED: resultTotal('COMPLETED'), OUT_OF_STOCK: resultTotal('OUT_OF_STOCK'), PAYMENT_FAILED: resultTotal('PAYMENT_FAILED'), UNFINISHED: unfinished },
			byKind: results,
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
		grafana: {
			from: new Date(setupData.startedAt).toISOString(),
			loadEnd: loadEnd === null ? null : new Date(loadEnd).toISOString(),
			to: teardownTime === null ? null : new Date(teardownTime).toISOString(),
			url: teardownTime === null ? null : `${GRAFANA_URL}/d/mega-order-saga?from=${setupData.startedAt}&to=${teardownTime}`,
		},
	};

	return {
		stdout: report(summary),
		[`loadtest/results/saga${LABEL ? '-' + LABEL : ''}-rate${RATE}-run${RUN}.json`]: JSON.stringify(summary, null, 2),
	};
}

function report(s) {
	const mark = (v) => (v ? 'PASS' : 'FAIL');
	const failedProducts = s.products.filter((p) => !p.pass).map((p) => `p${p.index}(기대 ${p.expected}, 실제 ${p.remaining})`);
	const failedUsers = s.users.filter((u) => !u.pass).map((u) => `u${u.index}(기대 ${u.expected}, 실제 ${u.remaining})`);
	const r = s.orders.byResult;
	return `
==================== 주문 Saga 부하 테스트 결과 ====================
${s.label ? s.label + ', ' : ''}RATE ${s.rate}/s × ${s.durationSeconds}s, RUN ${s.run}, VU ${s.vus}

[주문]  접수 ${s.orders.accepted} (${s.orders.acceptedPerSec}/s), 거절 ${s.orders.rejected}, 못 보낸 요청(dropped) ${s.orders.droppedIterations}
  결과: COMPLETED ${r.COMPLETED} / OUT_OF_STOCK ${r.OUT_OF_STOCK} / PAYMENT_FAILED ${r.PAYMENT_FAILED} / 미완료 ${r.UNFINISHED}
  접수 응답 avg / p95 / p99 / max : ${s.createLatencyMs.avg} / ${s.createLatencyMs.p95} / ${s.createLatencyMs.p99} / ${s.createLatencyMs.max} ms
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
