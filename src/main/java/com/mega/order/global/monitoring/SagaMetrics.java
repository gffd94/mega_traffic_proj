package com.mega.order.global.monitoring;

import com.mega.order.global.outbox.OutboxEventRepository;
import com.mega.order.global.outbox.OutboxEventStatus;
import com.mega.order.order.CancelReason;
import com.mega.order.order.OrderRepository;
import com.mega.order.order.OrderStatus;
import com.mega.order.order.OrderStatusCount;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Saga 모니터링 지표(이름은 모두 mega_ 접두사)를 등록하고 기록하는 곳.
 * 업무 코드는 "무슨 일이 있었는지"만 알려 주고, 지표 이름·태그·기록 시점은 여기서만 정한다.
 *
 * 주문 완료/취소 지표는 트랜잭션이 커밋된 뒤(afterCommit)에만 기록한다.
 * 상태 변경 직후에 바로 올리면, 커밋이 실패하거나(락 대기 시간 초과 등) 롤백된 뒤 재시도될 때
 * DB에는 한 번만 반영됐는데 지표는 두 번 오르게 된다.
 *
 * Counter와 Timer는 태그 조합까지 미리 등록해 둔다. 처음 기록될 때 생기게 두면
 * 앱 시작 직후에는 시계열이 없어서 Grafana의 rate()가 "0"이 아니라 "데이터 없음"으로 보인다.
 */
@Component
public class SagaMetrics {

	// 한 번의 스크레이프 안에서 상태별 Gauge 4개가 group by 결과 하나를 같이 쓰도록 잠깐 보관한다.
	// 스크레이프 간격(5초)보다 충분히 짧아서 다음 스크레이프에는 새로 조회한다.
	private static final Duration ORDER_STATUS_CACHE_TTL = Duration.ofSeconds(1);

	private final OrderRepository orderRepository;
	private final OutboxEventRepository outboxEventRepository;

	private final Counter ordersCompleted;
	private final Map<CancelReason, Counter> ordersCanceled = new EnumMap<>(CancelReason.class);
	private final Timer sagaCompletedDuration;
	private final Timer sagaCanceledDuration;
	private final Counter relayPublished;
	private final Counter relayConfirmFailed;
	private final Counter relayReturned;
	private final DistributionSummary relayBatchSize;
	private final Timer relayBatchDuration;

	private Map<OrderStatus, Long> orderStatusCounts = Map.of();
	private long orderStatusCountedAtNanos;
	private boolean orderStatusCounted;

	public SagaMetrics(MeterRegistry registry, OrderRepository orderRepository,
			OutboxEventRepository outboxEventRepository) {
		this.orderRepository = orderRepository;
		this.outboxEventRepository = outboxEventRepository;

		Gauge.builder("mega.outbox.ready", this, SagaMetrics::countReadyOutboxEvents)
				.description("아직 RabbitMQ로 보내지 않은 Outbox 이벤트 수 (스크레이프 시점 count 쿼리)")
				.register(registry);
		for (OrderStatus status : OrderStatus.values()) {
			Gauge.builder("mega.orders.by.status", this, metrics -> metrics.countOrders(status))
					.description("상태별 주문 수 (스크레이프 시점 group by 쿼리)")
					.tag("status", status.name())
					.register(registry);
		}

		this.ordersCompleted = Counter.builder("mega.orders.completed")
				.description("COMPLETED로 바뀐 주문 수 (커밋 후 기록)")
				.register(registry);
		for (CancelReason reason : CancelReason.values()) {
			ordersCanceled.put(reason, Counter.builder("mega.orders.canceled")
					.description("CANCELED로 바뀐 주문 수 (커밋 후 기록)")
					.tag("reason", reason.name())
					.register(registry));
		}

		this.sagaCompletedDuration = sagaDurationTimer(registry, "completed");
		this.sagaCanceledDuration = sagaDurationTimer(registry, "canceled");

		this.relayPublished = Counter.builder("mega.outbox.relay.published")
				.description("브로커가 받고(ack) 큐까지 라우팅된 Outbox 이벤트 수")
				.register(registry);
		this.relayConfirmFailed = relayFailedCounter(registry, "confirm_failed");
		this.relayReturned = relayFailedCounter(registry, "returned");
		this.relayBatchSize = DistributionSummary.builder("mega.outbox.relay.batch.size")
				.description("Relay가 한 번에 조회해 처리한 이벤트 수 (빈 조회는 기록하지 않음)")
				.register(registry);
		this.relayBatchDuration = Timer.builder("mega.outbox.relay.batch.duration")
				.description("Relay 배치 1회 처리 시간: 조회 + 전송 + confirm 대기 + PUBLISHED 표시 (빈 조회는 기록하지 않음)")
				.publishPercentileHistogram()
				.minimumExpectedValue(Duration.ofMillis(1))
				.maximumExpectedValue(Duration.ofSeconds(10))
				.register(registry);
	}

	/** 주문이 실제로 COMPLETED로 바뀌었을 때(complete()가 true) 같은 트랜잭션 안에서 호출한다. */
	public void orderCompleted(LocalDateTime orderedAt) {
		afterCommit(() -> {
			ordersCompleted.increment();
			sagaCompletedDuration.record(elapsedSince(orderedAt));
		});
	}

	/** 주문이 실제로 CANCELED로 바뀌었을 때(cancel()이 true) 같은 트랜잭션 안에서 호출한다. */
	public void orderCanceled(CancelReason reason, LocalDateTime orderedAt) {
		afterCommit(() -> {
			ordersCanceled.get(reason).increment();
			sagaCanceledDuration.record(elapsedSince(orderedAt));
		});
	}

	/** 이벤트를 1건 이상 가져온 배치가 끝날 때마다 호출한다 */
	public void relayBatch(int size, Duration elapsed) {
		relayBatchSize.record(size);
		relayBatchDuration.record(elapsed);
	}

	public void relayPublished() {
		relayPublished.increment();
	}

	/** nack, confirm 대기 시간 초과, 연결 오류 등 브로커가 받았다는 확인을 얻지 못한 경우 */
	public void relayConfirmFailed() {
		relayConfirmFailed.increment();
	}

	/** 브로커는 받았지만(ack) 어떤 큐에도 라우팅되지 않아 되돌아온 경우 (mandatory return) */
	public void relayReturned() {
		relayReturned.increment();
	}

	private void afterCommit(Runnable action) {
		// 트랜잭션 밖에서 불렸다면 되돌려질 변경이 없으므로 바로 기록한다
		if (!TransactionSynchronizationManager.isSynchronizationActive()) {
			action.run();
			return;
		}
		TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
			@Override
			public void afterCommit() {
				action.run();
			}
		});
	}

	// createdAt은 JPA Auditing이 같은 JVM 시계(LocalDateTime.now())로 채운 값이라 그대로 빼도 된다
	private Duration elapsedSince(LocalDateTime orderedAt) {
		Duration elapsed = Duration.between(orderedAt, LocalDateTime.now());
		return elapsed.isNegative() ? Duration.ZERO : elapsed;
	}

	private double countReadyOutboxEvents() {
		return outboxEventRepository.countByStatus(OutboxEventStatus.READY);
	}

	private synchronized double countOrders(OrderStatus status) {
		long now = System.nanoTime();
		if (!orderStatusCounted || now - orderStatusCountedAtNanos > ORDER_STATUS_CACHE_TTL.toNanos()) {
			Map<OrderStatus, Long> counts = new EnumMap<>(OrderStatus.class);
			for (OrderStatusCount row : orderRepository.countGroupByStatus()) {
				counts.put(row.status(), row.count());
			}
			orderStatusCounts = counts;
			orderStatusCountedAtNanos = now;
			orderStatusCounted = true;
		}
		return orderStatusCounts.getOrDefault(status, 0L);
	}

	private Timer sagaDurationTimer(MeterRegistry registry, String outcome) {
		return Timer.builder("mega.order.saga.duration")
				.description("주문 접수(createdAt)부터 COMPLETED/CANCELED 커밋까지 걸린 시간")
				.tag("outcome", outcome)
				.publishPercentileHistogram()
				// 부하 중 큐가 밀리면 수십 초~수 분까지 늘어날 수 있어 기본 상한(30초)보다 넓힌다
				.minimumExpectedValue(Duration.ofMillis(10))
				.maximumExpectedValue(Duration.ofMinutes(5))
				.register(registry);
	}

	private Counter relayFailedCounter(MeterRegistry registry, String reason) {
		return Counter.builder("mega.outbox.relay.failed")
				.description("Outbox 이벤트 발행 실패 수 (READY로 남아 다음 주기에 재시도)")
				.tag("reason", reason)
				.register(registry);
	}

}
