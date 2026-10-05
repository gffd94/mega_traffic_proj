package com.mega.order.user;

import com.mega.order.global.BaseTimeEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Entity
@Table(name = "users")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class User extends BaseTimeEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, length = 50)
	private String name;

	@Column(nullable = false, precision = 12, scale = 2)
	private BigDecimal pointBalance = BigDecimal.ZERO;

	@Builder
	private User(String name, BigDecimal pointBalance) {
		validateName(name);
		validatePointBalance(pointBalance);
		this.name = name;
		this.pointBalance = pointBalance != null ? pointBalance : BigDecimal.ZERO;
	}

	public void chargePoint(BigDecimal amount) {
		if (amount == null || amount.signum() <= 0) {
			throw new IllegalArgumentException("충전 금액은 0보다 커야 합니다. 입력 금액: " + amount);
		}
		this.pointBalance = this.pointBalance.add(amount);
	}

	private void validateName(String name) {
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException("회원 이름은 필수입니다.");
		}
	}

	private void validatePointBalance(BigDecimal pointBalance) {
		if (pointBalance != null && pointBalance.signum() < 0) {
			throw new IllegalArgumentException("포인트 잔액은 0 이상이어야 합니다. 입력 잔액: " + pointBalance);
		}
	}

}
