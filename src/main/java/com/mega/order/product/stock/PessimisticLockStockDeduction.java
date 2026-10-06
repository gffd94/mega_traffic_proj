package com.mega.order.product.stock;

import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.product.Product;
import com.mega.order.product.ProductRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * SELECT ... FOR UPDATE로 행 락을 잡고 조회한 뒤 변경 감지로 차감한다.
 */
@Component
@RequiredArgsConstructor
public class PessimisticLockStockDeduction implements StockDeductionStrategy {

	private final ProductRepository productRepository;

	@Override
	public String name() {
		return "pessimistic";
	}

	@Override
	@Transactional
	public void deduct(Long productId, int quantity) {
		Product product = productRepository.findByIdForUpdate(productId)
				.orElseThrow(() -> new ProductNotFoundException(List.of(productId)));
		product.decreaseStock(quantity);
	}

}
