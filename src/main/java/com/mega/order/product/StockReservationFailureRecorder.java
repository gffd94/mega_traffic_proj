package com.mega.order.product;

import com.mega.order.global.idempotency.ProcessedEventRecorder;
import com.mega.order.global.outbox.AggregateType;
import com.mega.order.global.outbox.OutboxEventRecorder;
import com.mega.order.global.outbox.OutboxEventTypes;
import com.mega.order.product.event.StockReserveFailedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 재고 예약 실패(StockReserveFailed)를 Outbox에 기록한다.
 *
 * 재고 예약 트랜잭션(StockReservationService.reserve) 안에서 기록하면 예외로 롤백될 때 이 기록도 같이 사라진다.
 * 그래서 리스너가 예외를 잡은 뒤, 롤백이 끝난 바깥에서 이 클래스를 호출해 새 트랜잭션으로 커밋한다.
 * StockReservationService 안의 메서드로 두면 같은 객체 내부 호출이라 프록시를 거치지 않아
 * @Transactional이 적용되지 않으므로 클래스를 나눈다.
 *
 * 예약 트랜잭션과 함께 처리 기록(processed_events)도 롤백됐으므로 여기서 다시 남긴다.
 * 이 기록이 있어야 같은 OrderCreated가 다시 왔을 때 (그 사이 재고가 채워졌더라도) 재고를 차감하지 않는다.
 */
@Component
@RequiredArgsConstructor
public class StockReservationFailureRecorder {

	private final OutboxEventRecorder outboxEventRecorder;
	private final ProcessedEventRecorder processedEventRecorder;

	// REQUIRES_NEW: 지금은 트랜잭션 없는 리스너에서만 부르지만, 호출한 쪽 트랜잭션과 섞이지 않는다는 의도를 드러낸다
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void record(Long orderId, String reason, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);
		// StockReserved와 같은 기준으로 aggregateId는 주문 ID로 둔다
		outboxEventRecorder.record(AggregateType.STOCK, orderId, OutboxEventTypes.STOCK_RESERVE_FAILED,
				new StockReserveFailedEvent(orderId, reason));
	}

}
