package com.mega.order.product.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.exception.InsufficientStockException;
import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.order.event.OrderCreatedEvent;
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

	private final StockReservationService stockReservationService;

	@RabbitListener(queues = SagaTopology.PRODUCT_ORDER_CREATED_QUEUE)
	public void handle(OrderCreatedEvent event, @Header(name = "eventId", required = false) String eventId) {
		log.info("OrderCreated 수신. eventId={}, orderId={}", eventId, event.orderId());
		try {
			stockReservationService.reserve(event);
		} catch (InsufficientStockException | ProductNotFoundException e) {
			// 비즈니스 실패라 재시도해도 결과가 같다. 트랜잭션은 이미 롤백됐고, 메시지는 정상 처리(ack)한다.
			// TODO 다음 단계에서 StockReserveFailed 이벤트를 발행해 주문을 CANCELED(OUT_OF_STOCK)로 바꾼다
			log.warn("재고 예약 실패. eventId={}, orderId={}, 이유={}", eventId, event.orderId(), e.getMessage());
		}
	}

}
