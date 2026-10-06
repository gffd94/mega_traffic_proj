package com.mega.order.product.stock;

import com.mega.order.global.exception.InsufficientStockException;
import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.product.Product;
import com.mega.order.product.ProductRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 조건부 UPDATE 한 문장으로 확인과 차감을 원자적으로 처리한다.
 */
@Component
@RequiredArgsConstructor
public class AtomicUpdateStockDeduction implements StockDeductionStrategy {

	private final ProductRepository productRepository;

	@Override
	public String name() {
		return "atomic";
	}

	@Override
	@Transactional
	public void deduct(Long productId, int quantity) {
		int updatedRows = productRepository.decreaseStockIfEnough(productId, quantity);
		if (updatedRows == 0) {
			// 0행은 상품이 없거나 재고가 부족한 경우. 다시 조회해서 구분한다.
			Product product = productRepository.findById(productId)
					.orElseThrow(() -> new ProductNotFoundException(List.of(productId)));
			throw new InsufficientStockException(quantity, product.getStockQuantity());
		}
	}

}
