package com.mega.order.order.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.order.OrderService;
import com.mega.order.product.event.StockReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderStockReservedListener {

	private final OrderService orderService;

	@RabbitListener(queues = SagaTopology.ORDER_STOCK_RESERVED_QUEUE)
	public void handle(StockReservedEvent event, @Header(name = "eventId", required = false) String eventId) {
		log.info("StockReserved 수신(order). eventId={}, orderId={}", eventId, event.orderId());
		boolean reserved = orderService.reserveStock(event.orderId());
		if (!reserved) {
			// PaymentCompleted가 먼저 처리됐거나 이미 취소된 주문. 잘못된 상태가 아니라 순서가 바뀐 것이므로 정상 처리한다.
			log.info("이미 끝난 주문이라 재고 예약 반영을 건너뜁니다. eventId={}, orderId={}", eventId, event.orderId());
		}
	}

}
