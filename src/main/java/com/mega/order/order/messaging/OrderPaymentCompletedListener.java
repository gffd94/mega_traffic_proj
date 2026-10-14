package com.mega.order.order.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.idempotency.DuplicateEventGuard;
import com.mega.order.order.OrderService;
import com.mega.order.payment.event.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderPaymentCompletedListener {

	private static final String CONSUMER = SagaTopology.ORDER_PAYMENT_COMPLETED_QUEUE;

	private final OrderService orderService;
	private final DuplicateEventGuard duplicateEventGuard;

	@RabbitListener(queues = SagaTopology.ORDER_PAYMENT_COMPLETED_QUEUE)
	public void handle(PaymentCompletedEvent event, @Header(name = "eventId") String eventId) {
		log.info("PaymentCompleted 수신. eventId={}, orderId={}", eventId, event.orderId());
		duplicateEventGuard.run(CONSUMER, eventId, () -> {
			boolean completed = orderService.complete(event.orderId(), CONSUMER, eventId);
			if (!completed) {
				log.info("이미 끝난 주문이라 주문 확정을 건너뜁니다. eventId={}, orderId={}", eventId, event.orderId());
			}
		});
	}

}
