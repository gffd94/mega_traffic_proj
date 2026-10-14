package com.mega.order.order;

import com.mega.order.global.exception.OrderNotFoundException;
import com.mega.order.global.exception.UserNotFoundException;
import com.mega.order.global.idempotency.ProcessedEventRecorder;
import com.mega.order.global.monitoring.SagaMetrics;
import com.mega.order.global.outbox.AggregateType;
import com.mega.order.global.outbox.OutboxEventRecorder;
import com.mega.order.global.outbox.OutboxEventTypes;
import com.mega.order.order.dto.OrderCreateRequest;
import com.mega.order.order.dto.OrderItemRequest;
import com.mega.order.order.dto.OrderResponse;
import com.mega.order.order.dto.OrderStatusResponse;
import com.mega.order.product.ProductInfo;
import com.mega.order.product.ProductReader;
import com.mega.order.user.UserReader;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class OrderService {

	private final OrderRepository orderRepository;
	private final OrderItemRepository orderItemRepository;
	private final OrderMapper orderMapper;
	private final UserReader userReader;
	private final ProductReader productReader;
	private final OutboxEventRecorder outboxEventRecorder;
	private final ProcessedEventRecorder processedEventRecorder;
	private final SagaMetrics sagaMetrics;

	@Transactional
	public OrderResponse create(String idempotencyKey, OrderCreateRequest request) {
		if (!userReader.existsById(request.userId())) {
			throw new UserNotFoundException(request.userId());
		}

		List<Long> productIds = request.items().stream()
				.map(OrderItemRequest::productId)
				.toList();
		validateNoDuplicateProduct(productIds);

		Map<Long, BigDecimal> priceByProductId = productReader.getProductInfos(productIds).stream()
				.collect(Collectors.toMap(ProductInfo::id, ProductInfo::price));

		BigDecimal totalPrice = request.items().stream()
				.map(item -> priceByProductId.get(item.productId()).multiply(BigDecimal.valueOf(item.quantity())))
				.reduce(BigDecimal.ZERO, BigDecimal::add);

		Order savedOrder = orderRepository.save(orderMapper.toEntity(idempotencyKey, request, totalPrice));

		List<OrderItem> orderItems = request.items().stream()
				.map(item -> orderMapper.toItemEntity(savedOrder, item, priceByProductId.get(item.productId())))
				.toList();
		orderItemRepository.saveAll(orderItems);

		// 주문과 같은 트랜잭션에서 기록해야 주문이 롤백되면 이벤트도 함께 사라진다
		outboxEventRecorder.record(AggregateType.ORDER, savedOrder.getId(), OutboxEventTypes.ORDER_CREATED,
				orderMapper.toCreatedEvent(savedOrder, orderItems));

		return orderMapper.toResponse(savedOrder);
	}

	/*
	 * 아래 세 메서드는 반영했으면 true, 늦게 온 이벤트라 무시했으면 false를 돌려준다.
	 * 무시할지는 Order가 판단한다 (Order의 상태 변경 메서드 주석 참고).
	 * 무시한 경우에도 처리 기록은 커밋된다. 같은 이벤트가 다시 와도 주문 행을 잠그기 전에 걸러진다.
	 * 완료/취소 지표는 실제로 반영한 경우(true)에만, 커밋된 뒤에 오른다 (SagaMetrics 참고).
	 */

	@Transactional
	public boolean reserveStock(Long orderId, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);
		return findOrderForUpdate(orderId).reserveStock();
	}

	@Transactional
	public boolean complete(Long orderId, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);
		Order order = findOrderForUpdate(orderId);
		boolean completed = order.complete();
		if (completed) {
			sagaMetrics.orderCompleted(order.getCreatedAt());
		}
		return completed;
	}

	@Transactional
	public boolean cancel(Long orderId, CancelReason reason, String consumer, String eventId) {
		processedEventRecorder.record(consumer, eventId);
		Order order = findOrderForUpdate(orderId);
		boolean canceled = order.cancel(reason);
		if (canceled) {
			sagaMetrics.orderCanceled(reason, order.getCreatedAt());
		}
		return canceled;
	}

	@Transactional(readOnly = true)
	public Optional<OrderResponse> findByIdempotencyKey(String idempotencyKey) {
		return orderRepository.findByIdempotencyKey(idempotencyKey)
				.map(orderMapper::toResponse);
	}

	@Transactional(readOnly = true)
	public OrderStatusResponse getStatus(Long orderId) {
		Order order = orderRepository.findById(orderId)
				.orElseThrow(() -> new OrderNotFoundException(orderId));
		return orderMapper.toStatusResponse(order);
	}

	// StockReserved와 PaymentCompleted/PaymentFailed는 서로 다른 큐로 와서 다른 스레드가 같은 주문을 동시에 바꿀 수 있다.
	// 행 락으로 순서를 세워서, 한쪽이 바꾼 상태를 다른 쪽이 옛 상태로 덮어쓰지 못하게 한다.
	private Order findOrderForUpdate(Long orderId) {
		return orderRepository.findByIdForUpdate(orderId)
				.orElseThrow(() -> new OrderNotFoundException(orderId));
	}

	private void validateNoDuplicateProduct(List<Long> productIds) {
		if (productIds.stream().distinct().count() != productIds.size()) {
			throw new IllegalArgumentException("같은 상품을 중복해서 주문할 수 없습니다. 수량을 합쳐서 요청해주세요.");
		}
	}

}
