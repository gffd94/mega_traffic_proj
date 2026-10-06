package com.mega.order.product.stock;

import com.mega.order.global.exception.ProductNotFoundException;
import com.mega.order.product.Product;
import com.mega.order.product.ProductRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 분산 락 안에서 실행되는 차감 1회. 동시성은 바깥의 Redisson 락이 막으므로 DB 락은 쓰지 않는다.
 */
@Component
@RequiredArgsConstructor
public class RedissonLockStockDeductionExecutor {

	private final ProductRepository productRepository;

	@Transactional
	public void deduct(Long productId, int quantity) {
		Product product = productRepository.findById(productId)
				.orElseThrow(() -> new ProductNotFoundException(List.of(productId)));
		product.decreaseStock(quantity);
	}

}
