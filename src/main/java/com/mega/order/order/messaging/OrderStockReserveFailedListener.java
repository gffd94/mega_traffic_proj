package com.mega.order.order.messaging;

import com.mega.order.global.config.SagaTopology;
import com.mega.order.global.idempotency.DuplicateEventGuard;
import com.mega.order.order.CancelReason;
import com.mega.order.order.OrderService;
import com.mega.order.product.event.StockReserveFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderStockReserveFailedListener {

	private static final String CONSUMER = SagaTopology.ORDER_STOCK_RESERVE_FAILED_QUEUE;

	private final OrderService orderService;
	private final DuplicateEventGuard duplicateEventGuard;

	@RabbitListener(queues = SagaTopology.ORDER_STOCK_RESERVE_FAILED_QUEUE)
	public void handle(StockReserveFailedEvent event, @Header(name = "eventId") String eventId) {
		log.info("StockReserveFailed 수신. eventId={}, orderId={}, 이유={}", eventId, event.orderId(), event.reason());
		duplicateEventGuard.run(CONSUMER, eventId, () -> {
			// 재고 부족과 상품 없음 모두 OUT_OF_STOCK으로 취소한다. 구체적인 원인은 event.reason에 남아 있다.
			boolean canceled = orderService.cancel(event.orderId(), CancelReason.OUT_OF_STOCK, CONSUMER, eventId);
			if (!canceled) {
				log.info("이미 끝난 주문이라 주문 취소를 건너뜁니다. eventId={}, orderId={}", eventId, event.orderId());
			}
		});
	}

}
