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
		this.name = name;
		this.pointBalance = pointBalance != null ? pointBalance : BigDecimal.ZERO;
	}

}
