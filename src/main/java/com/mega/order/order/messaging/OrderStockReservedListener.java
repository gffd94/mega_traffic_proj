package com.mega.order.order.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.idempotency.DuplicateEventGuard;
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

	private static final String CONSUMER = SagaTopology.ORDER_STOCK_RESERVED_QUEUE;

	private final OrderService orderService;
	private final DuplicateEventGuard duplicateEventGuard;

	@RabbitListener(queues = SagaTopology.ORDER_STOCK_RESERVED_QUEUE)
	public void handle(StockReservedEvent event, @Header(name = "eventId") String eventId) {
		log.info("StockReserved 수신(order). eventId={}, orderId={}", eventId, event.orderId());
		duplicateEventGuard.run(CONSUMER, eventId, () -> {
			boolean reserved = orderService.reserveStock(event.orderId(), CONSUMER, eventId);
			if (!reserved) {
				// PaymentCompleted/PaymentFailed가 먼저 처리된 경우. 잘못된 상태가 아니라 순서가 바뀐 것이므로 정상 처리한다.
				log.info("이미 끝났거나 재고 예약이 반영된 주문이라 건너뜁니다. eventId={}, orderId={}", eventId, event.orderId());
			}
		});
	}

}
