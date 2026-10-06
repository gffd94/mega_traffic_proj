package com.mega.order.product;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProductRepository extends JpaRepository<Product, Long> {

	@Modifying(clearAutomatically = true, flushAutomatically = true)
	// 벌크 UPDATE는 JPA Auditing을 거치지 않으므로 updatedAt을 직접 갱신한다.
	@Query("UPDATE Product p SET p.stockQuantity = p.stockQuantity - :quantity, p.updatedAt = CURRENT_TIMESTAMP "
			+ "WHERE p.id = :id AND p.stockQuantity >= :quantity")
	int decreaseStockIfEnough(@Param("id") Long id, @Param("quantity") int quantity);

	@Lock(LockModeType.PESSIMISTIC_WRITE)
	@Query("SELECT p FROM Product p WHERE p.id = :id")
	Optional<Product> findByIdForUpdate(@Param("id") Long id);

}
