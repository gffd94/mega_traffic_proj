package com.mega.order.product.stock;

/**
 * 재고 차감 동시성 실험용 전략. 주문/결제 흐름과는 연결하지 않는다.
 * 재고가 부족하면 InsufficientStockException, 상품이 없으면 ProductNotFoundException을 던진다.
 */
public interface StockDeductionStrategy {

	String name();

	void deduct(Long productId, int quantity);

}
