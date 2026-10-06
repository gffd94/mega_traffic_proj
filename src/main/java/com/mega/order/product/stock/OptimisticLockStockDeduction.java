package com.mega.order.product.stock;

import com.mega.order.global.exception.StockDeductionConflictException;
import lombok.RequiredArgsConstructor;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;

/**
 * 트랜잭션 없이 재시도만 지휘한다. 충돌한 트랜잭션이 롤백된 뒤 새 트랜잭션으로 다시 시도해야
 * 최신 version을 읽을 수 있으므로 Executor와 클래스를 나눈다.
 * 재고 부족(InsufficientStockException)은 재시도해도 결과가 같으므로 잡지 않는다.
 */
@Component
@RequiredArgsConstructor
public class OptimisticLockStockDeduction implements StockDeductionStrategy {

	private static final int MAX_RETRIES = 3;

	private final OptimisticLockStockDeductionExecutor executor;

	@Override
	public String name() {
		return "optimistic";
	}

	@Override
	public void deduct(Long productId, int quantity) {
		ObjectOptimisticLockingFailureException lastConflict = null;
		for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
			try {
				executor.deduct(productId, quantity);
				return;
			} catch (ObjectOptimisticLockingFailureException e) {
				lastConflict = e;
			}
		}
		throw new StockDeductionConflictException(productId, MAX_RETRIES, lastConflict);
	}

}
