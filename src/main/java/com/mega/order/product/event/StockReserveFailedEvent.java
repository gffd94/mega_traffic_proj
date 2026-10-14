package com.mega.order.product.event;

/**
 * 재고 예약 트랜잭션은 통째로 롤백된 뒤에 발행되므로, 차감된 재고가 없어 복구할 items는 담지 않는다.
 */
public record StockReserveFailedEvent(
		Long orderId,
		String reason
) {
}
