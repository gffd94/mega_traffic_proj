package com.mega.order.order;

import com.mega.order.order.dto.OrderCreateRequest;
import com.mega.order.order.dto.OrderResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

/**
 * 트랜잭션 없이 호출 순서만 지휘한다.
 * 같은 Idempotency-Key 동시 요청의 unique 위반은 OrderService 트랜잭션이 롤백된 뒤 여기서 잡아야
 * 새 트랜잭션으로 먼저 커밋된 주문을 다시 조회할 수 있다.
 */
@Component
@RequiredArgsConstructor
public class OrderFacade {

	private final OrderService orderService;

	public OrderResponse create(String idempotencyKey, OrderCreateRequest request) {
		return orderService.findByIdempotencyKey(idempotencyKey)
				.orElseGet(() -> createOrFindExisting(idempotencyKey, request));
	}

	private OrderResponse createOrFindExisting(String idempotencyKey, OrderCreateRequest request) {
		try {
			return orderService.create(idempotencyKey, request);
		} catch (DataIntegrityViolationException e) {
			return orderService.findByIdempotencyKey(idempotencyKey)
					.orElseThrow(() -> e);
		}
	}

}
