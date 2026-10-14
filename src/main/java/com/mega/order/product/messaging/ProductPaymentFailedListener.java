package com.mega.order.product.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.idempotency.DuplicateEventGuard;
import com.mega.order.payment.event.PaymentFailedEvent;
import com.mega.order.product.StockReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductPaymentFailedListener {

	private static final String CONSUMER = SagaTopology.PRODUCT_PAYMENT_FAILED_QUEUE;

	private final StockReservationService stockReservationService;
	private final DuplicateEventGuard duplicateEventGuard;

	@RabbitListener(queues = SagaTopology.PRODUCT_PAYMENT_FAILED_QUEUE)
	public void handle(PaymentFailedEvent event, @Header(name = "eventId") String eventId) {
		log.info("PaymentFailed 수신(product). eventId={}, orderId={}", eventId, event.orderId());
		// 재고 복구는 조건 없이 더하므로 같은 이벤트를 두 번 반영하면 재고가 두 번 늘어난다. 처리 기록으로 막는다.
		duplicateEventGuard.run(CONSUMER, eventId, () -> stockReservationService.release(event, CONSUMER, eventId));
	}

}
