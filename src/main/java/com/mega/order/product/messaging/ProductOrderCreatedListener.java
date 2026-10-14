package com.mega.order.product.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.exception.InsufficientStockException;
import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.global.idempotency.DuplicateEventGuard;
import com.mega.order.order.event.OrderCreatedEvent;
import com.mega.order.product.StockReservationFailureRecorder;
import com.mega.order.product.StockReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductOrderCreatedListener {

	private static final String CONSUMER = SagaTopology.PRODUCT_ORDER_CREATED_QUEUE;

	private final StockReservationService stockReservationService;
	private final StockReservationFailureRecorder stockReservationFailureRecorder;
	private final DuplicateEventGuard duplicateEventGuard;

	@RabbitListener(queues = SagaTopology.PRODUCT_ORDER_CREATED_QUEUE)
	public void handle(OrderCreatedEvent event, @Header(name = "eventId") String eventId) {
		log.info("OrderCreated 수신. eventId={}, orderId={}", eventId, event.orderId());
		duplicateEventGuard.run(CONSUMER, eventId, () -> {
			try {
				stockReservationService.reserve(event, CONSUMER, eventId);
			} catch (InsufficientStockException | ProductNotFoundException e) {
				// 비즈니스 실패라 재시도해도 결과가 같다. 재고 차감과 처리 기록은 이미 전부 롤백됐다.
				// 실패 사실은 롤백이 끝난 뒤 새 트랜잭션으로 기록해야 남는다. 기록이 실패하면 예외가 그대로 나가 재시도된다.
				log.warn("재고 예약 실패. eventId={}, orderId={}, 이유={}", eventId, event.orderId(), e.getMessage());
				stockReservationFailureRecorder.record(event.orderId(), e.getMessage(), CONSUMER, eventId);
			}
		});
	}

}
