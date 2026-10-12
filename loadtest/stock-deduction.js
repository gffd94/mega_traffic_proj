// 재고 차감 방식 비교 부하 테스트
//
// 실행 (프로젝트 루트에서):
//   k6 run -e STRATEGY=atomic -e SCENARIO=A loadtest/stock-deduction.js
//
// -e 옵션
//   STRATEGY : none | atomic | pessimistic | optimistic | redisson (필수)
//   SCENARIO : A (한정 수량 쟁탈) | B (지속 부하) (필수)
//   BASE_URL : 기본값 http://localhost:8080
//
// 결과 요약은 콘솔에 출력하고 loadtest/results/{STRATEGY}-{SCENARIO}-{시각}.json 으로 저장한다.

import http from 'k6/http';
import { Counter, Gauge, Trend } from 'k6/metrics';

const STRATEGIES = ['none', 'atomic', 'pessimistic', 'optimistic', 'redisson'];

const SCENARIOS = {
	A: {
		description: '한정 수량 쟁탈: 재고 100, VU 200명이 각 1회 요청',
		initialStock: 100,
		executor: {
			executor: 'per-vu-iterations',
			vus: 200,
			iterations: 1,
			maxDuration: '1m',
		},
	},
	B: {
		description: '지속 부하: 재고 100000, VU 100명이 30초 동안 계속 요청',
		initialStock: 100000,
		executor: {
			executor: 'constant-vus',
			vus: 100,
			duration: '30s',
		},
	},
};

const STRATEGY = __ENV.STRATEGY;
const SCENARIO = (__ENV.SCENARIO || '').toUpperCase();
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

if (!STRATEGIES.includes(STRATEGY)) {
	throw new Error(`STRATEGY는 ${STRATEGIES.join(', ')} 중 하나여야 합니다. 입력: ${STRATEGY}`);
}
if (!SCENARIOS[SCENARIO]) {
	throw new Error(`SCENARIO는 A 또는 B여야 합니다. 입력: ${__ENV.SCENARIO}`);
}

const scenario = SCENARIOS[SCENARIO];
const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

// 응답 코드별 개수. 차감 요청에서만 기록하므로 setup/teardown 요청은 섞이지 않는다.
const deductSuccess = new Counter('deduct_200_success');
const deductOutOfStock = new Counter('deduct_409_out_of_stock');
const deductConflict = new Counter('deduct_503_conflict');
const deductOther = new Counter('deduct_other');
// 그 외 중에서 응답 자체를 받지 못한 경우 (연결 거부, 타임아웃 등 status 0)
const deductConnectionError = new Counter('deduct_other_connection_error');
// 차감 요청만의 응답 시간 (http_req_duration에는 setup/teardown 요청도 섞인다).
// 응답을 받지 못한 요청은 걸린 시간이 0에 가까워 평균을 왜곡하므로 넣지 않는다.
const deductDuration = new Trend('deduct_duration', true);
// teardown에서 조회한 값을 handleSummary로 넘기기 위한 지표
const remainingStockGauge = new Gauge('remaining_stock');
const deductPhaseMs = new Gauge('deduct_phase_ms');

export const options = {
	scenarios: {
		[`stock_${SCENARIO}`]: scenario.executor,
	},
	summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
};

// 409(재고 부족)와 503(충돌/락 대기 초과)은 실험에서 예상된 응답이므로 http_req_failed에 넣지 않는다.
http.setResponseCallback(http.expectedStatuses(200, 409, 503));

export function setup() {
	const res = http.post(
		`${BASE_URL}/api/v1/products`,
		JSON.stringify({
			name: `loadtest-${STRATEGY}-${SCENARIO}-${Date.now()}`,
			price: 1000,
			stockQuantity: scenario.initialStock,
		}),
		{ ...JSON_HEADERS, tags: { name: 'setup_create_product' } },
	);
	if (res.status !== 201) {
		throw new Error(`상품 등록 실패: status=${res.status}, body=${res.body}`);
	}
	return {
		productId: res.json('id'),
		deductStartedAt: Date.now(),
	};
}

export default function (data) {
	const res = http.post(
		`${BASE_URL}/api/v1/experiments/stock/${STRATEGY}`,
		JSON.stringify({ productId: data.productId, quantity: 1 }),
		{ ...JSON_HEADERS, tags: { name: 'deduct' } },
	);

	if (res.status === 0) {
		deductOther.add(1);
		deductConnectionError.add(1);
		return;
	}

	deductDuration.add(res.timings.duration);
	if (res.status === 200) {
		deductSuccess.add(1);
	} else if (res.status === 409) {
		deductOutOfStock.add(1);
	} else if (res.status === 503) {
		deductConflict.add(1);
	} else {
		deductOther.add(1);
	}
}

export function teardown(data) {
	// 모든 VU가 끝난 직후 호출되므로, 여기까지의 시간이 차감 구간의 길이다.
	deductPhaseMs.add(Date.now() - data.deductStartedAt);

	const res = http.get(`${BASE_URL}/api/v1/products/${data.productId}`, { tags: { name: 'teardown_get_product' } });
	if (res.status === 200) {
		remainingStockGauge.add(res.json('stockQuantity'));
	} else {
		console.error(`남은 재고 조회 실패: status=${res.status}, body=${res.body}`);
	}
}

// teardown은 HTTP 요청은 할 수 있지만 집계된 지표(성공 수 등)를 읽을 수 없고,
// handleSummary는 집계된 지표를 받지만 HTTP 요청을 할 수 없다.
// 그래서 teardown이 남은 재고를 지표로 기록하고, 판정은 handleSummary에서 한다.
export function handleSummary(data) {
	const count = (name) => (data.metrics[name] ? data.metrics[name].values.count : 0);
	const gauge = (name) => (data.metrics[name] ? data.metrics[name].values.value : null);
	const trend = data.metrics.deduct_duration ? data.metrics.deduct_duration.values : {};

	const success = count('deduct_200_success');
	const outOfStock = count('deduct_409_out_of_stock');
	const conflict = count('deduct_503_conflict');
	const other = count('deduct_other');
	const connectionError = count('deduct_other_connection_error');
	const total = success + outOfStock + conflict + other;
	const remainingStock = gauge('remaining_stock');
	const phaseMs = gauge('deduct_phase_ms');

	const expectedRemaining = scenario.initialStock - success;
	const checks = remainingStock === null
		? { stockMatchesSuccess: null, stockNotNegative: null, passed: false }
		: {
			stockMatchesSuccess: expectedRemaining === remainingStock,
			stockNotNegative: remainingStock >= 0,
		};
	if (remainingStock !== null) {
		checks.passed = checks.stockMatchesSuccess && checks.stockNotNegative;
	}

	const summary = {
		strategy: STRATEGY,
		scenario: SCENARIO,
		scenarioDescription: scenario.description,
		baseUrl: BASE_URL,
		finishedAt: new Date().toISOString(),
		productId: data.setup_data ? data.setup_data.productId : null,
		initialStock: scenario.initialStock,
		responses: { total, success, outOfStock409: outOfStock, conflict503: conflict, other, otherConnectionError: connectionError },
		remainingStock,
		expectedRemainingStock: expectedRemaining,
		checks,
		latencyMs: {
			avg: round(trend.avg),
			p95: round(trend['p(95)']),
			p99: round(trend['p(99)']),
			max: round(trend.max),
		},
		deductPhaseSeconds: phaseMs === null ? null : round(phaseMs / 1000),
		rps: phaseMs ? round(total / (phaseMs / 1000)) : null,
	};

	return {
		stdout: report(summary),
		[`loadtest/results/${STRATEGY}-${SCENARIO}-${timestamp()}.json`]: JSON.stringify(summary, null, 2),
	};
}

function report(s) {
	const mark = (v) => (v === null ? '확인 불가' : v ? 'PASS' : 'FAIL');
	return `
==================== 재고 차감 부하 테스트 결과 ====================
방식 / 시나리오   : ${s.strategy} / ${s.scenario} (${s.scenarioDescription})
상품 ID           : ${s.productId}

[응답 코드별 개수]  총 ${s.responses.total}건
  200 성공            : ${s.responses.success}
  409 재고 부족       : ${s.responses.outOfStock409}
  503 충돌/락 대기 초과: ${s.responses.conflict503}
  그 외               : ${s.responses.other} (그중 연결 실패: ${s.responses.otherConnectionError})

[재고 정합성]
  처음 재고 ${s.initialStock} − 성공 ${s.responses.success} = ${s.expectedRemainingStock}, 실제 남은 재고 = ${s.remainingStock}
  ① 처음 재고 − 성공 수 = 남은 재고 : ${mark(s.checks.stockMatchesSuccess)}
  ② 남은 재고 ≥ 0                   : ${mark(s.checks.stockNotNegative)}
  판정                               : ${mark(s.checks.passed)}

[성능] (차감 요청만 집계)
  응답 시간 avg / p95 / p99 / max : ${s.latencyMs.avg} / ${s.latencyMs.p95} / ${s.latencyMs.p99} / ${s.latencyMs.max} ms
  차감 구간 ${s.deductPhaseSeconds}s 동안 RPS : ${s.rps}
====================================================================
`;
}

function round(v) {
	return typeof v === 'number' ? Math.round(v * 100) / 100 : null;
}

function timestamp() {
	const d = new Date();
	const pad = (n) => String(n).padStart(2, '0');
	return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}
