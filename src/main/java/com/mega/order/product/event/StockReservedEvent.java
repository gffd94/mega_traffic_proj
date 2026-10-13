package com.mega.order.product.event;

import java.math.BigDecimal;
import java.util.List;

/**
 * items는 결제가 실패했을 때 재고를 되돌리는 데 쓰려고 그대로 전달한다.
 * product가 order 테이블을 조회하지 않고도 무엇을 얼마나 복구할지 알 수 있게 하기 위해서다.
 */
public record StockReservedEvent(
		Long orderId,
		Long userId,
		BigDecimal totalPrice,
		List<Item> items
) {

	public record Item(
			Long productId,
			int quantity
	) {
	}

}
