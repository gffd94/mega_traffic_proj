package com.mega.order.product.stock;

import com.mega.order.global.exception.ProductNotFoundException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 낙관적 락 차감 1회를 하나의 트랜잭션으로 수행한다.
 * version이 바뀌었으면 커밋 시점에 ObjectOptimisticLockingFailureException이 난다.
 */
@Component
@RequiredArgsConstructor
public class OptimisticLockStockDeductionExecutor {

	private final VersionedProductRepository versionedProductRepository;

	@Transactional
	public void deduct(Long productId, int quantity) {
		VersionedProduct product = versionedProductRepository.findById(productId)
				.orElseThrow(() -> new ProductNotFoundException(List.of(productId)));
		product.decreaseStock(quantity);
	}

}
