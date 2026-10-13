package com.mega.order.product;

import com.mega.order.global.exception.InsufficientStockException;
import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.global.outbox.AggregateType;
import com.mega.order.global.outbox.OutboxEventRecorder;
import com.mega.order.global.outbox.OutboxEventTypes;
import com.mega.order.order.event.OrderCreatedEvent;
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

	/**
	 * 주문 상품 전체의 재고를 한 트랜잭션에서 차감하고 StockReserved를 Outbox에 기록한다.
	 * 상품 하나라도 차감에 실패하면 예외가 나서 앞서 차감한 상품까지 모두 롤백된다.
	 */
	@Transactional
	public void reserve(OrderCreatedEvent event) {
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
