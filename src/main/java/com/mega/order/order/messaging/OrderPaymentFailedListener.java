package com.mega.order.order.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.idempotency.DuplicateEventGuard;
import com.mega.order.order.CancelReason;
import com.mega.order.order.OrderService;
import com.mega.order.payment.event.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderPaymentFailedListener {

	private static final String CONSUMER = SagaTopology.ORDER_PAYMENT_FAILED_QUEUE;

	private final OrderService orderService;
	private final DuplicateEventGuard duplicateEventGuard;

	@RabbitListener(queues = SagaTopology.ORDER_PAYMENT_FAILED_QUEUE)
	public void handle(PaymentFailedEvent event, @Header(name = "eventId") String eventId) {
		log.info("PaymentFailed 수신(order). eventId={}, orderId={}, 이유={}", eventId, event.orderId(), event.reason());
		duplicateEventGuard.run(CONSUMER, eventId, () -> {
			boolean canceled = orderService.cancel(event.orderId(), CancelReason.PAYMENT_FAILED, CONSUMER, eventId);
			if (!canceled) {
				log.info("이미 끝난 주문이라 주문 취소를 건너뜁니다. eventId={}, orderId={}", eventId, event.orderId());
			}
		});
	}

}
