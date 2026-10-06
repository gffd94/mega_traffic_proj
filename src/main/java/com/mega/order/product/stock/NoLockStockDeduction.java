package com.mega.order.product.stock;

import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.product.Product;
import com.mega.order.product.ProductRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 비교 기준. 일부러 락이나 조건 없이 조회 후 변경 감지로 차감한다.
 */
@Component
@RequiredArgsConstructor
public class NoLockStockDeduction implements StockDeductionStrategy {

	private final ProductRepository productRepository;

	@Override
	public String name() {
		return "none";
	}

	@Override
	@Transactional
	public void deduct(Long productId, int quantity) {
		Product product = productRepository.findById(productId)
				.orElseThrow(() -> new ProductNotFoundException(List.of(productId)));
		product.decreaseStock(quantity);
	}

}
