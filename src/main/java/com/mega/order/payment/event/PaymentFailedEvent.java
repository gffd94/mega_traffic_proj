package com.mega.order.payment.event;

import java.util.List;

/**
 * items는 product가 앞 단계에서 차감한 재고를 되돌리는 데 쓴다 (StockReservedEvent의 items를 그대로 옮긴다).
 */
public record PaymentFailedEvent(
		Long orderId,
		Long userId,
		List<Item> items,
		String reason
) {

	public record Item(
			Long productId,
			int quantity
	) {
	}

}
