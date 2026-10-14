package com.mega.order.order;

import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OrderRepository extends JpaRepository<Order, Long> {

	Optional<Order> findByIdempotencyKey(String idempotencyKey);

	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("SELECT o FROM Order o WHERE o.id = :id")
	Optional<Order> findByIdForUpdate(@Param("id") Long id);

	// 주문이 하나도 없는 상태는 결과에 나오지 않는다 (호출하는 쪽에서 0으로 채운다)
	@Query("SELECT new com.mega.order.order.OrderStatusCount(o.status, COUNT(o)) FROM Order o GROUP BY o.status")
	List<OrderStatusCount> countGroupByStatus();

}
