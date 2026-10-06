package com.mega.order.product.stock;

import com.mega.order.global.exception.StockDeductionConflictException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

/**
 * 트랜잭션 없이 락만 지휘한다. 락이 트랜잭션을 바깥에서 감싸야 커밋이 끝난 뒤에 락이 풀려
 * 다음 요청이 커밋된 재고를 읽는다. 그래서 트랜잭션은 Executor에 둔다.
 */
@Component
@RequiredArgsConstructor
public class RedissonLockStockDeduction implements StockDeductionStrategy {

	private static final String LOCK_KEY_PREFIX = "lock:product:";
	private static final Duration LOCK_WAIT_TIME = Duration.ofSeconds(5);

	private final RedissonClient redissonClient;
	private final RedissonLockStockDeductionExecutor executor;

	@Override
	public String name() {
		return "redisson";
	}

	@Override
	public void deduct(Long productId, int quantity) {
		RLock lock = redissonClient.getLock(LOCK_KEY_PREFIX + productId);
		// leaseTime을 지정하지 않아 watchdog이 작업 중 락 만료를 연장한다.
		if (!tryLock(lock)) {
			throw new StockDeductionConflictException(productId, LOCK_WAIT_TIME);
		}
		try {
			executor.deduct(productId, quantity);
		} finally {
			if (lock.isHeldByCurrentThread()) {
				lock.unlock();
			}
		}
	}

	private boolean tryLock(RLock lock) {
		try {
			return lock.tryLock(LOCK_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("락 획득 대기 중 인터럽트되었습니다.", e);
		}
	}

}
