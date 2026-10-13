package com.mega.order.order.messaging;

import com.mega.order.global.config.SagaTopology;
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

	private final OrderService orderService;

	@RabbitListener(queues = SagaTopology.ORDER_PAYMENT_COMPLETED_QUEUE)
	public void handle(PaymentCompletedEvent event, @Header(name = "eventId", required = false) String eventId) {
		log.info("PaymentCompleted 수신. eventId={}, orderId={}", eventId, event.orderId());
		orderService.complete(event.orderId());
	}

}
