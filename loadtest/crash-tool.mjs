// crash-run.sh가 쓰는 보조 도구: 장애 순간의 DB·큐 상태를 찍고, 실행이 끝나면 근거를 모아 결과 JSON에 덧붙인다.
//
// node loadtest/crash-tool.mjs <명령> ...
//   publish-in                    : exchange mega.saga가 받은 publish 누계 (관리 API, 최대 5초 늦은 통계)
//   queues <out.json> [processed] : Saga 큐의 메시지를 꺼내 보고 다시 넣는다(requeue). eventId와 redelivered를 저장.
//                                   processed를 주면 그 메시지들의 처리 기록(processed_events)도 함께 저장 (앱이 죽어 있을 때)
//   outbox-ready <out.json>       : READY Outbox 이벤트와, 그 이벤트를 이미 처리한 컨슈머 기록을 저장 (앱이 죽어 있을 때 호출)
//   counts <out.json>             : 브로커의 현재 큐별 메시지 수 (rabbitmqctl, 통계 지연 없음)
//   wait-consumers                : 모든 Saga 큐에 컨슈머가 다시 붙을 때까지 기다리고 그 시각(ms)을 출력
//   analyze <mode> <run> <dir>    : <dir>의 기록과 DB·Prometheus를 모아 loadtest/results/crash-<mode>-run<run>.json을 보강
//
// 환경 변수: .env의 MYSQL_ROOT_PASSWORD, MYSQL_DATABASE, RABBITMQ_USER, RABBITMQ_PASSWORD (crash-run.sh가 넘겨준다)

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const RABBIT_API = process.env.RABBIT_API || 'http://localhost:15672/api';
const PROMETHEUS_URL = process.env.PROMETHEUS_URL || 'http://localhost:9090';
const EXCHANGE = 'mega.saga';
const DLQ = 'mega.saga.dlq';

// eventType별로 그 이벤트를 받아야 하는 컨슈머(= 큐 이름). RabbitMqConfig의 바인딩과 같다
const CONSUMERS_BY_EVENT_TYPE = {
	OrderCreated: ['product.order-created'],
	StockReserved: ['payment.stock-reserved', 'order.stock-reserved'],
	PaymentCompleted: ['order.payment-completed'],
	StockReserveFailed: ['order.stock-reserve-failed'],
	PaymentFailed: ['product.payment-failed', 'order.payment-failed'],
};
const QUEUES = [...new Set(Object.values(CONSUMERS_BY_EVENT_TYPE).flat())];

// 리스너가 처리를 시작할 때 남기는 로그의 앞부분 (큐별)
const RECEIVE_LOG_PREFIX = {
	'product.order-created': 'OrderCreated 수신.',
	'payment.stock-reserved': 'StockReserved 수신(payment).',
	'order.stock-reserved': 'StockReserved 수신(order).',
	'order.payment-completed': 'PaymentCompleted 수신.',
	'order.stock-reserve-failed': 'StockReserveFailed 수신.',
	'product.payment-failed': 'PaymentFailed 수신(product).',
	'order.payment-failed': 'PaymentFailed 수신(order).',
};

const [command, ...args] = process.argv.slice(2);
const commands = { 'publish-in': publishIn, queues, counts, 'outbox-ready': outboxReady, 'wait-consumers': waitConsumers, analyze };
if (!commands[command]) {
	console.error(`알 수 없는 명령: ${command}`);
	process.exit(2);
}
await commands[command](...args);

// ---------------------------------------------------------------- 명령

async function publishIn() {
	const exchange = await rabbit(`/exchanges/%2F/${EXCHANGE}`);
	console.log(exchange.message_stats?.publish_in ?? 0);
}

async function queues(out, withProcessed) {
	const takenAt = Date.now();
	const result = {};
	for (const queue of [...QUEUES, DLQ]) {
		const messages = await rabbit(`/queues/%2F/${encodeURIComponent(queue)}/get`, {
			method: 'POST',
			body: JSON.stringify({ count: 100000, ackmode: 'ack_requeue_true', encoding: 'auto', truncate: 100 }),
		});
		result[queue] = messages.map((m) => ({ eventId: m.properties?.headers?.eventId ?? null, redelivered: m.redelivered }));
	}
	const snapshot = { takenAt, queues: result };
	if (withProcessed === 'processed') {
		const ids = [...new Set(Object.values(result).flat().map((m) => m.eventId).filter(Boolean))];
		snapshot.processed = ids.length === 0 ? [] : sql(
			`SELECT consumer, event_id FROM processed_events WHERE event_id IN (${ids.map(quote).join(',')})`,
		).map(([consumer, eventId]) => `${consumer} ${eventId}`);
	}
	writeFileSync(out, JSON.stringify(snapshot, null, 2));
	console.log(Object.entries(result).map(([q, ms]) => `${q}=${ms.length}`).join(' '));
}

async function counts(out) {
	const takenAt = Date.now();
	const queues = rabbitctlQueues();
	writeFileSync(out, JSON.stringify({ takenAt, queues }, null, 2));
	console.log(Object.entries(queues).map(([q, c]) => `${q}=${c.messages}(unacked ${c.unacked})`).join(' '));
}

async function outboxReady(out) {
	const takenAt = Date.now();
	const ready = sql(`SELECT event_id, event_type, ROUND(UNIX_TIMESTAMP(created_at) * 1000) FROM outbox_events WHERE status = 'READY'`)
		.map(([eventId, eventType, createdAt]) => ({ eventId, eventType, createdAt: Number(createdAt) }));
	const processed = ready.length === 0 ? [] : sql(
		`SELECT consumer, event_id FROM processed_events WHERE event_id IN (${ready.map((e) => quote(e.eventId)).join(',')})`,
	).map(([consumer, eventId]) => ({ consumer, eventId }));
	writeFileSync(out, JSON.stringify({ takenAt, ready, processed }, null, 2));
	console.log(`READY ${ready.length}, 이미 처리된 기록 ${processed.length}`);
}

async function waitConsumers() {
	for (;;) {
		try {
			const list = await rabbit('/queues/%2F?columns=name,consumers');
			const consumers = Object.fromEntries(list.map((q) => [q.name, q.consumers]));
			if (QUEUES.every((q) => consumers[q] > 0)) {
				console.log(Date.now());
				return;
			}
		} catch {
			// 브로커가 아직 뜨는 중
		}
		await sleep(250);
	}
}

// ---------------------------------------------------------------- 분석

async function analyze(mode, run, dir) {
	const resultFile = `loadtest/results/crash-${mode}-run${run}.json`;
	const summary = JSON.parse(readFileSync(resultFile, 'utf8'));
	const timing = readTiming(path.join(dir, 'timing.env'));
	const { loadStart, loadEnd, drainEnd, teardown } = summary.times;
	const startedAtMs = Date.parse(summary.grafana.from);
	const windowSeconds = Math.ceil((teardown - startedAtMs) / 1000) + 5;

	const userIds = summary.users.map((u) => u.id);
	const appLogs = readdirSync(dir).filter((f) => /^app-\d+\.log$/.test(f)).sort().map((f) => ({ file: f, text: readFileSync(path.join(dir, f), 'utf8') }));

	// ①② 장부: k6가 202를 받은 주문(ACCEPTED 줄) 중 DB에서 COMPLETED인 것의 수량·금액만큼 줄었어야 한다
	const acceptedOrders = readK6Lines(path.join(dir, 'k6.log'), 'ACCEPTED');
	const acceptedIds = new Set(acceptedOrders.map((o) => String(o.orderId)));
	const dbOrders = sql(`SELECT id, status, COALESCE(cancel_reason, ''), user_id FROM orders WHERE user_id IN (${userIds.join(',')})`)
		.map(([id, status, cancelReason, userId]) => ({ id, status, cancelReason, userId: Number(userId) }));
	const statusById = Object.fromEntries(dbOrders.map((o) => [o.id, o.status]));
	const completedQty = {};
	const completedAmount = {};
	for (const o of acceptedOrders) {
		if (statusById[String(o.orderId)] === 'COMPLETED') {
			completedQty[o.p] = (completedQty[o.p] || 0) + o.q;
			completedAmount[o.u] = (completedAmount[o.u] || 0) + o.a;
		}
	}
	const stockNow = Object.fromEntries(sql(`SELECT id, stock_quantity FROM products WHERE id IN (${summary.products.map((p) => p.id).join(',')})`).map(([id, s]) => [id, Number(s)]));
	const pointNow = Object.fromEntries(sql(`SELECT id, point_balance FROM users WHERE id IN (${userIds.join(',')})`).map(([id, b]) => [id, Number(b)]));
	const products = summary.products.map((p) => {
		const expected = p.initialStock - (completedQty[p.index] || 0);
		return { index: p.index, id: p.id, initialStock: p.initialStock, completedQuantity: completedQty[p.index] || 0, expected, remaining: stockNow[p.id], pass: stockNow[p.id] === expected };
	});
	const users = summary.users.map((u) => {
		const expected = u.initialPoint - (completedAmount[u.index] || 0);
		return { index: u.index, id: u.id, initialPoint: u.initialPoint, completedAmount: completedAmount[u.index] || 0, expected, remaining: pointNow[u.id], pass: pointNow[u.id] === expected };
	});
	// DB에는 있지만 k6가 202를 받지 못한 주문 (응답을 잃고 재시도도 끝내 실패한 경우 등)
	const unknownOrders = dbOrders.filter((o) => !acceptedIds.has(o.id));

	// ⑤ 주문 건수 = k6가 202를 받은 고유 Idempotency-Key 수
	const [orderCount, distinctKeys] = sql(`SELECT COUNT(*), COUNT(DISTINCT idempotency_key) FROM orders WHERE user_id IN (${userIds.join(',')})`)[0].map(Number);
	// ③ (DB 기준) 이번 실행 회원의 주문 중 진행 중인 것
	const dbUnfinished = Number(sql(`SELECT COUNT(*) FROM orders WHERE user_id IN (${userIds.join(',')}) AND status IN ('PENDING', 'STOCK_RESERVED')`)[0][0]);
	// ④ DLQ (관리 API 통계가 아닌 브로커의 현재 값)
	const dlq = rabbitctlQueues()[DLQ]?.messages ?? null;

	// 이벤트 전달 누락: 이번 실행 동안 기록된 Outbox 이벤트 × 받아야 하는 컨슈머마다 처리 기록이 있는지
	const consumerMap = Object.entries(CONSUMERS_BY_EVENT_TYPE)
		.flatMap(([type, consumers]) => consumers.map((c) => `SELECT ${quote(type)} AS t, ${quote(c)} AS c`)).join(' UNION ALL ');
	const since = `FROM_UNIXTIME(${startedAtMs / 1000})`;
	const [outboxEvents, readyLeft] = sql(`SELECT COUNT(*), COALESCE(SUM(status = 'READY'), 0) FROM outbox_events WHERE created_at >= ${since}`)[0].map(Number);
	const [expectedDeliveries, missingDeliveries] = sql(`
		SELECT COUNT(*), COALESCE(SUM(p.id IS NULL), 0)
		FROM outbox_events o JOIN (${consumerMap}) m ON m.t = o.event_type
		LEFT JOIN processed_events p ON p.consumer = m.c AND p.event_id = o.event_id
		WHERE o.created_at >= ${since}`)[0].map(Number);

	// ⑥ 중복 건너뜀: Prometheus(단계·컨슈머별)와 앱 로그(단계별)
	const promDuplicates = await promVector(`sum by (consumer, stage) (increase(mega_idempotency_duplicate_skipped_total[${windowSeconds}s] @ ${Math.ceil(teardown / 1000)}))`);
	const skipLines = appLogs.flatMap(({ file, text }) => [...text.matchAll(/이미 처리된 이벤트라 건너뜁니다\. consumer=(\S+), eventId=(\S+), stage=(\w+)/g)]
		.map((m) => ({ file, consumer: m[1], eventId: m[2], stage: m[3] })));
	const logDuplicates = countBy(skipLines, (l) => `${l.consumer} ${l.stage}`);

	const prom = async (q) => round(await promScalar(`${q}`), 2);
	const crash = {
		timing,
		db: { orderCount, distinctIdempotencyKeys: distinctKeys, unfinished: dbUnfinished, outboxEvents, outboxReadyLeft: readyLeft, expectedDeliveries, missingDeliveries },
		dlqNow: dlq,
		duplicateSkipped: {
			prometheus: Object.fromEntries(promDuplicates.filter((r) => r.value > 0).map((r) => [`${r.metric.consumer} ${r.metric.stage}`, round(r.value, 1)])),
			prometheusTotal: round(promDuplicates.reduce((s, r) => s + r.value, 0), 1),
			appLog: logDuplicates,
			appLogTotal: skipLines.length,
		},
		relayFailed: await prom(`sum(increase(mega_outbox_relay_failed_total[${windowSeconds}s] @ ${Math.ceil(teardown / 1000)}))`),
		appLogErrors: Object.fromEntries(appLogs.map(({ file, text }) => [file, {
			error: (text.match(/ ERROR /g) || []).length,
			warn: (text.match(/ WARN /g) || []).length,
			dlqRecover: (text.match(/메시지 처리를 포기하고 DLQ로 보냅니다/g) || []).length,
			relaySendFailed: (text.match(/Outbox 이벤트 전송 실패/g) || []).length,
			confirmTimeout: (text.match(/confirm 대기 시간을 넘겼습니다/g) || []).length,
			confirmFailed: (text.match(/confirm을 받지 못했습니다/g) || []).length,
		}])),
	};

	if (mode === 'app') {
		Object.assign(crash, analyzeAppCrash({ dir, timing, summary, appLogs, skipLines, outboxEvents }));
		crash.recoveryMs = { restartToReady: timing.readyAt - timing.restartAt, readyToInProgressZero: drainEnd ? drainEnd - timing.readyAt : null };
	} else {
		Object.assign(crash, analyzeMqRestart({ dir, timing }));
		crash.recoveryMs = {
			restartToBrokerUp: timing.mqUpAt - timing.mqRestartAt,
			restartToConsumersBack: timing.consumersBackAt - timing.mqRestartAt,
			brokerUpToInProgressZero: drainEnd ? drainEnd - timing.mqUpAt : null,
		};
	}

	crash.ledger = {
		acceptedLogged: acceptedOrders.length,
		unknownOrders: unknownOrders.map((o) => ({ id: o.id, status: o.status, cancelReason: o.cancelReason || null })),
		failedProducts: products.filter((p) => !p.pass),
		failedUsers: users.filter((u) => !u.pass),
	};

	const checks = {
		// ① ② ③ ④ ⑤ ⑥ 순서
		stockLedger: products.every((p) => p.pass),
		pointLedger: users.every((u) => u.pass),
		// k6가 접수한 주문의 최종 상태(DB)와 이번 실행 회원의 DB 주문 전체에 진행 중이 없고, drain이 시간 안에 끝남
		noUnfinishedOrders: dbUnfinished === 0 && acceptedOrders.every((o) => ['COMPLETED', 'CANCELED'].includes(statusById[String(o.orderId)]))
			&& summary.drainSeconds !== null,
		dlqEmpty: dlq === 0,
		ordersMatchAcceptedKeys: acceptedOrders.length === summary.orders.accepted && orderCount === acceptedIds.size
			&& unknownOrders.length === 0 && distinctKeys === orderCount,
		duplicateSkipped: crash.duplicateSkipped.appLogTotal,
		// 보조 판정: 이번 실행의 Outbox 이벤트가 받아야 할 컨슈머 모두에게 처리됐고 READY로 남은 것이 없음
		noMissingDeliveries: missingDeliveries === 0 && readyLeft === 0,
	};
	checks.passed = checks.stockLedger && checks.pointLedger && checks.noUnfinishedOrders && checks.dlqEmpty
		&& checks.ordersMatchAcceptedKeys && checks.noMissingDeliveries;
	summary.k6Checks = summary.checks;
	summary.checks = checks;
	summary.products = products;
	summary.users = users;
	summary.crash = crash;
	writeFileSync(resultFile, JSON.stringify(summary, null, 2));
	console.log(JSON.stringify({ checks, crash: { ...crash, appLogErrors: undefined } }, null, 2));
}

function analyzeAppCrash({ dir, timing, summary, appLogs, skipLines, outboxEvents }) {
	const killAt = timing.killAt;
	const beforeKillLog = appLogs[0].text;

	// 1. 접수 중: 재시도 끝에 접수된 주문 중 첫 실패가 "broken"(연결 후 끊김)인 것.
	//    주문이 죽기 전에 이미 커밋돼 있었으면 응답만 잃어버린 것(재시도가 기존 주문을 돌려받음)
	const retried = readK6Lines(path.join(dir, 'k6.log'), 'RETRIED_ACCEPT');
	const ids = retried.map((r) => r.orderId);
	const createdAt = ids.length === 0 ? {} : Object.fromEntries(
		sql(`SELECT id, ROUND(UNIX_TIMESTAMP(created_at) * 1000) FROM orders WHERE id IN (${ids.join(',')})`).map(([id, t]) => [id, Number(t)]));
	const broken = retried.filter((r) => r.firstFailure === 'broken');
	const accepting = {
		retriedAccepted: retried.length,
		firstFailure: countBy(retried, (r) => r.firstFailure),
		inFlightAtKill: broken.length,
		committedBeforeKillResponseLost: broken.filter((r) => createdAt[r.orderId] < killAt).map((r) => ({ orderId: r.orderId, createdAt: createdAt[r.orderId], firstError: r.firstError })),
		rolledBackThenCreatedByRetry: broken.filter((r) => createdAt[r.orderId] >= killAt).length,
		refusedOrdersCreatedBeforeKill: retried.filter((r) => r.firstFailure !== 'broken' && createdAt[r.orderId] < killAt).length,
	};

	// 2~4. 죽은 직후 찍은 READY 이벤트와 큐 내용
	const ready = JSON.parse(readFileSync(path.join(dir, 'snap-outbox-ready.json'), 'utf8'));
	const queueSnap = JSON.parse(readFileSync(path.join(dir, 'snap-queues.json'), 'utf8'));
	const inQueue = new Set(Object.values(queueSnap.queues).flat().map((m) => m.eventId));
	const processedIds = new Set(ready.processed.map((p) => p.eventId));
	// 죽은 직후 큐에 있던 메시지 중, 그 컨슈머의 처리 기록이 이미 커밋돼 있던 것
	const processedPairs = new Set(queueSnap.processed);
	const sentNotMarked = ready.ready.filter((e) => inQueue.has(e.eventId) || processedIds.has(e.eventId));

	// 재시작 후 Relay가 다시 보낸 이벤트: 브로커가 받은 publish 수 − 이번 실행 Outbox 이벤트 수
	const resentByBroker = timing.publishInAfter != null ? timing.publishInAfter - timing.publishInBefore - outboxEvents : null;

	const redelivered = Object.entries(queueSnap.queues).filter(([q]) => q !== DLQ)
		.flatMap(([queue, ms]) => ms.filter((m) => m.redelivered).map((m) => ({ queue, eventId: m.eventId })));
	const receivedBeforeKill = (r) => beforeKillLog.includes(`${RECEIVE_LOG_PREFIX[r.queue]} eventId=${r.eventId}`);
	const committedNotAcked = redelivered.filter((r) => processedPairs.has(`${r.queue} ${r.eventId}`));
	const notCommitted = redelivered.filter((r) => !committedNotAcked.includes(r));
	const afterRestartSkips = new Set(skipLines.filter((l) => l.file !== appLogs[0].file).map((l) => `${l.consumer} ${l.eventId}`));

	return {
		crashPoints: {
			accepting,
			relaySentNotMarked: {
				readyAtKill: ready.ready.length,
				sentNotMarked: sentNotMarked.length,
				sentNotMarkedInQueue: sentNotMarked.filter((e) => inQueue.has(e.eventId)).length,
				sentNotMarkedProcessed: sentNotMarked.filter((e) => processedIds.has(e.eventId)).length,
				eventTypes: countBy(sentNotMarked, (e) => e.eventType),
				resentByBroker,
			},
			consumerProcessing: {
				redeliveredAtKill: redelivered.length,
				startedProcessing: notCommitted.filter(receivedBeforeKill).map((r) => `${r.queue} ${r.eventId}`),
				prefetchedOnly: notCommitted.filter((r) => !receivedBeforeKill(r)).length,
			},
			committedNotAcked: committedNotAcked.map((r) => ({ ...r, skippedAfterRestart: afterRestartSkips.has(`${r.queue} ${r.eventId}`) })),
			queueSnapshotTakenMsAfterKill: queueSnap.takenAt - killAt,
			outboxSnapshotTakenMsAfterKill: ready.takenAt - killAt,
		},
	};
}

function analyzeMqRestart({ dir }) {
	const before = JSON.parse(readFileSync(path.join(dir, 'snap-queues.json'), 'utf8'));
	const counts = (file) => existsSync(path.join(dir, file)) ? JSON.parse(readFileSync(path.join(dir, file), 'utf8')) : null;
	const peeked = Object.entries(before.queues).filter(([q]) => q !== DLQ).flatMap(([queue, ms]) => ms.map((m) => ({ queue, eventId: m.eventId })));
	const ids = [...new Set(peeked.map((p) => p.eventId))];
	const processed = ids.length === 0 ? new Set() : new Set(
		sql(`SELECT consumer, event_id FROM processed_events WHERE event_id IN (${ids.map(quote).join(',')})`).map(([c, e]) => `${c} ${e}`));
	return {
		queuesAroundRestart: {
			countsBefore: counts('queues-before.json'),
			countsAfter: counts('queues-after.json'),
			peekedBeforeRestart: peeked.length,
			peekedLaterProcessed: peeked.filter((p) => processed.has(`${p.queue} ${p.eventId}`)).length,
			peekedLost: peeked.filter((p) => !processed.has(`${p.queue} ${p.eventId}`)).map((p) => `${p.queue} ${p.eventId}`),
		},
	};
}

// ---------------------------------------------------------------- 도구

function readTiming(file) {
	const timing = {};
	for (const line of readFileSync(file, 'utf8').split('\n')) {
		const m = line.match(/^(\w+)=(.*)$/);
		if (m) {
			timing[m[1]] = /^-?\d+$/.test(m[2]) ? Number(m[2]) : m[2];
		}
	}
	return timing;
}

function readK6Lines(file, prefix) {
	if (!existsSync(file)) {
		return [];
	}
	// k6를 --log-format=raw로 실행해서 console.log 내용이 그대로 한 줄씩 남는다
	return [...readFileSync(file, 'utf8').matchAll(new RegExp(`^${prefix} (\\{.*\\})\\s*$`, 'gm'))].map((m) => JSON.parse(m[1]));
}

function sql(query) {
	const out = execFileSync('docker', ['exec', '-i', '-e', 'MYSQL_PWD', 'mega-mysql', 'mysql', '-uroot', '-N', '-B', process.env.MYSQL_DATABASE], {
		input: query,
		env: { ...process.env, MYSQL_PWD: process.env.MYSQL_ROOT_PASSWORD },
		encoding: 'utf8',
	});
	return out.split('\n').filter((l) => l.length > 0).map((l) => l.split('\t'));
}

function rabbitctlQueues() {
	const out = execFileSync('docker', ['exec', 'mega-rabbitmq', 'rabbitmqctl', 'list_queues', '-q', '--no-table-headers', 'name', 'messages', 'messages_ready', 'messages_unacknowledged'], { encoding: 'utf8' });
	return Object.fromEntries(out.split('\n').filter(Boolean).map((l) => l.trim().split(/\s+/)).map(([name, messages, ready, unacked]) => [name, { messages: Number(messages), ready: Number(ready), unacked: Number(unacked) }]));
}

async function rabbit(apiPath, init = {}) {
	const auth = Buffer.from(`${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASSWORD}`).toString('base64');
	const res = await fetch(`${RABBIT_API}${apiPath}`, { ...init, headers: { Authorization: `Basic ${auth}`, 'Content-Type': 'application/json' } });
	if (!res.ok) {
		throw new Error(`RabbitMQ API ${apiPath}: ${res.status}`);
	}
	return res.json();
}

async function promVector(query) {
	const res = await fetch(`${PROMETHEUS_URL}/api/v1/query?query=${encodeURIComponent(query)}`);
	const body = await res.json();
	return (body.data?.result || []).map((r) => ({ metric: r.metric, value: Number(r.value[1]) }));
}

async function promScalar(query) {
	const vector = await promVector(query);
	return vector.length === 0 ? null : vector[0].value;
}

function countBy(items, keyOf) {
	const counts = {};
	for (const item of items) {
		const key = keyOf(item);
		counts[key] = (counts[key] || 0) + 1;
	}
	return counts;
}

function quote(s) {
	return `'${String(s).replace(/'/g, "''")}'`;
}

function round(v, digits = 2) {
	return typeof v === 'number' && Number.isFinite(v) ? Math.round(v * 10 ** digits) / 10 ** digits : null;
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
