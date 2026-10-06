package com.mega.order.order;

import com.mega.order.global.exception.OrderNotFoundException;
import com.mega.order.global.exception.UserNotFoundException;
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

		return orderMapper.toResponse(savedOrder);
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

	private void validateNoDuplicateProduct(List<Long> productIds) {
		if (productIds.stream().distinct().count() != productIds.size()) {
			throw new IllegalArgumentException("같은 상품을 중복해서 주문할 수 없습니다. 수량을 합쳐서 요청해주세요.");
		}
	}

}
