package com.mega.order.payment.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.exception.InsufficientPointException;
import com.mega.order.global.exception.UserNotFoundException;
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

	private final PaymentService paymentService;

	@RabbitListener(queues = SagaTopology.PAYMENT_STOCK_RESERVED_QUEUE)
	public void handle(StockReservedEvent event, @Header(name = "eventId", required = false) String eventId) {
		log.info("StockReserved 수신(payment). eventId={}, orderId={}", eventId, event.orderId());
		try {
			paymentService.pay(event);
		} catch (InsufficientPointException | UserNotFoundException e) {
			// 비즈니스 실패라 재시도해도 결과가 같다. 트랜잭션은 이미 롤백됐고, 메시지는 정상 처리(ack)한다.
			// TODO 다음 단계에서 결제 FAILED 기록 + PaymentFailed 이벤트를 발행해 재고 복구와 주문 취소를 이어 붙인다.
			//  지금은 앞 단계에서 차감한 재고가 복구되지 않은 채 남는다.
			log.warn("결제 실패. eventId={}, orderId={}, 이유={}", eventId, event.orderId(), e.getMessage());
		}
	}

}
