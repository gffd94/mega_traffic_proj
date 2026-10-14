package com.mega.order.payment.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.exception.InsufficientPointException;
import com.mega.order.global.exception.UserNotFoundException;
import com.mega.order.global.idempotency.DuplicateEventGuard;
import com.mega.order.payment.PaymentFailureRecorder;
import com.mega.order.payment.PaymentService;
import com.mega.order.product.event.StockReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentStockReservedListener {

	private static final String CONSUMER = SagaTopology.PAYMENT_STOCK_RESERVED_QUEUE;

	private final PaymentService paymentService;
	private final PaymentFailureRecorder paymentFailureRecorder;
	private final DuplicateEventGuard duplicateEventGuard;

	@RabbitListener(queues = SagaTopology.PAYMENT_STOCK_RESERVED_QUEUE)
	public void handle(StockReservedEvent event, @Header(name = "eventId") String eventId) {
		log.info("StockReserved 수신(payment). eventId={}, orderId={}", eventId, event.orderId());
		duplicateEventGuard.run(CONSUMER, eventId, () -> {
			try {
				paymentService.pay(event, CONSUMER, eventId);
			} catch (InsufficientPointException | UserNotFoundException e) {
				// 비즈니스 실패라 재시도해도 결과가 같다. 포인트 차감과 처리 기록은 이미 롤백됐다.
				// 실패 사실은 롤백이 끝난 뒤 새 트랜잭션으로 기록해야 남는다. 기록이 실패하면 예외가 그대로 나가 재시도된다.
				log.warn("결제 실패. eventId={}, orderId={}, 이유={}", eventId, event.orderId(), e.getMessage());
				paymentFailureRecorder.record(event, e.getMessage(), CONSUMER, eventId);
			}
		});
	}

}
