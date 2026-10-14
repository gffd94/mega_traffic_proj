package com.mega.order.product;

import com.mega.order.global.exception.InsufficientStockException;
import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.global.idempotency.ProcessedEventRecorder;
import com.mega.order.global.outbox.AggregateType;
import com.mega.order.global.outbox.OutboxEventRecorder;
import com.mega.order.global.outbox.OutboxEventTypes;
import com.mega.order.order.event.OrderCreatedEvent;
import com.mega.order.payment.event.PaymentFailedEvent;
import com.mega.order.product.event.StockReservedEvent;
import java.util.Comparator;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class StockReservationService {

	private final ProductRepository productRepository;
	private final OutboxEventRecorder outboxEventRecorder;
	private final ProcessedEventRecorder processedEventRecorder;

	/**
	 * 처리 기록을 먼저 남긴 뒤, 주문 상품 전체의 재고를 한 트랜잭션에서 차감하고 StockReserved를 Outbox에 기록한다.
	 * 상품 하나라도 차감에 실패하면 예외가 나서 앞서 차감한 상품과 처리 기록까지 모두 롤백된다.
	 * 실패 이벤트(StockReserveFailed)는 여기서 기록하지 않는다. 이 트랜잭션과 함께 롤백되기 때문이다 (StockReservationFailureRecorder 참고).
	 */
	@Transactional
	public void reserve(OrderCreatedEvent event, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);

		// 모든 주문이 같은 순서(상품 ID 오름차순)로 행 락을 잡게 해서 데드락을 막는다
		List<OrderCreatedEvent.Item> sortedItems = event.items().stream()
				.sorted(Comparator.comparing(OrderCreatedEvent.Item::productId))
				.toList();
		for (OrderCreatedEvent.Item item : sortedItems) {
			decreaseStock(item.productId(), item.quantity());
		}

		List<StockReservedEvent.Item> reservedItems = sortedItems.stream()
				.map(item -> new StockReservedEvent.Item(item.productId(), item.quantity()))
				.toList();
		// 재고 예약을 따로 저장하는 엔티티가 없으므로 aggregateId는 주문 ID로 둔다
		outboxEventRecorder.record(AggregateType.STOCK, event.orderId(), OutboxEventTypes.STOCK_RESERVED,
				new StockReservedEvent(event.orderId(), event.userId(), event.totalPrice(), reservedItems));
	}

	/**
	 * 결제 실패에 대한 보상으로, 예약 때 차감한 재고를 모두 되돌린다.
	 * 예약 때와 같은 순서(상품 ID 오름차순)로 행 락을 잡아, 동시에 진행 중인 재고 차감과 데드락이 나지 않게 한다.
	 */
	@Transactional
	public void release(PaymentFailedEvent event, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);

		List<PaymentFailedEvent.Item> sortedItems = event.items().stream()
				.sorted(Comparator.comparing(PaymentFailedEvent.Item::productId))
				.toList();
		for (PaymentFailedEvent.Item item : sortedItems) {
			int updatedRows = productRepository.increaseStock(item.productId(), item.quantity());
			if (updatedRows == 0) {
				// 예약 때 있던 상품이 사라진 경우. 비즈니스 실패가 아니라 데이터 이상이므로 재시도 후 에러 로그로 남긴다.
				throw new ProductNotFoundException(List.of(item.productId()));
			}
		}
	}

	private void decreaseStock(Long productId, int quantity) {
		int updatedRows = productRepository.decreaseStockIfEnough(productId, quantity);
		if (updatedRows == 0) {
			// 0행은 상품이 없거나 재고가 부족한 경우. 다시 조회해서 구분한다.
			Product product = productRepository.findById(productId)
					.orElseThrow(() -> new ProductNotFoundException(List.of(productId)));
			throw new InsufficientStockException(quantity, product.getStockQuantity());
		}
	}

}
