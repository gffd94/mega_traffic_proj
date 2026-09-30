package com.mega.order.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Getter
@Entity
@Table(name = "payments")
@EntityListeners(AuditingEntityListener.class)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Payment {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, unique = true)
	private Long orderId;

	@Column(nullable = false, precision = 12, scale = 2)
	private BigDecimal amount;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, columnDefinition = "varchar(20)")
	private PaymentStatus status;

	@Column(length = 255)
	private String failedReason;

	@CreatedDate
	@Column(nullable = false, updatable = false)
	private LocalDateTime createdAt;

	private Payment(Long orderId, BigDecimal amount, PaymentStatus status, String failedReason) {
		this.orderId = orderId;
		this.amount = amount;
		this.status = status;
		this.failedReason = failedReason;
	}

	public static Payment success(Long orderId, BigDecimal amount) {
		return new Payment(orderId, amount, PaymentStatus.SUCCESS, null);
	}

	public static Payment failed(Long orderId, BigDecimal amount, String failedReason) {
		if (failedReason == null) {
			throw new IllegalArgumentException("결제 실패 사유는 필수입니다.");
		}
		return new Payment(orderId, amount, PaymentStatus.FAILED, failedReason);
	}

}
