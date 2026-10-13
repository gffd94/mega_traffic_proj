package com.mega.order.user;

import java.math.BigDecimal;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface UserRepository extends JpaRepository<User, Long> {

	@Modifying(clearAutomatically = true, flushAutomatically = true)
	// 벌크 UPDATE는 JPA Auditing을 거치지 않으므로 updatedAt을 직접 갱신한다.
	@Query("UPDATE User u SET u.pointBalance = u.pointBalance - :amount, u.updatedAt = CURRENT_TIMESTAMP "
			+ "WHERE u.id = :id AND u.pointBalance >= :amount")
	int decreasePointIfEnough(@Param("id") Long id, @Param("amount") BigDecimal amount);

}
