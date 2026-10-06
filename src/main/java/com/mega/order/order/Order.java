package com.mega.order.order;

import com.mega.order.global.BaseTimeEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
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
@Table(name = "orders")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order extends BaseTimeEntity {

	// total_price 컬럼 decimal(12,2)의 최대값
	private static final BigDecimal MAX_TOTAL_PRICE = new BigDecimal("9999999999.99");

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false)
	private Long userId;

	@Column(nullable = false, unique = true, length = 64)
	private String idempotencyKey;

	@Column(nullable = false, precision = 12, scale = 2)
	private BigDecimal totalPrice;

	@Enumerated(EnumType.STRING)
	@Column(nullable = false, columnDefinition = "varchar(20)")
	private OrderStatus status;

	@Enumerated(EnumType.STRING)
	@Column(columnDefinition = "varchar(50)")
	private CancelReason cancelReason;

	@Builder
	private Order(Long userId, String idempotencyKey, BigDecimal totalPrice) {
		validateTotalPrice(totalPrice);
		this.userId = userId;
		this.idempotencyKey = idempotencyKey;
		this.totalPrice = totalPrice;
		this.status = OrderStatus.PENDING;
	}

	public void reserveStock() {
		if (this.status != OrderStatus.PENDING) {
			throw new IllegalStateException("현재 상태 " + this.status + "에서는 재고 예약을 할 수 없습니다.");
		}
		this.status = OrderStatus.STOCK_RESERVED;
	}

	public void complete() {
		if (this.status != OrderStatus.PENDING && this.status != OrderStatus.STOCK_RESERVED) {
			throw new IllegalStateException("현재 상태 " + this.status
					+ "에서는 주문 확정을 할 수 없습니다. (PENDING 또는 STOCK_RESERVED 상태에서만 가능)");
		}
		this.status = OrderStatus.COMPLETED;
	}

	public void cancel(CancelReason reason) {
		if (reason == null) {
			throw new IllegalArgumentException("취소 사유는 필수입니다.");
		}
		if (this.status != OrderStatus.PENDING && this.status != OrderStatus.STOCK_RESERVED) {
			throw new IllegalStateException("현재 상태 " + this.status + "에서는 주문 취소를 할 수 없습니다.");
		}
		this.status = OrderStatus.CANCELED;
		this.cancelReason = reason;
	}

	private void validateTotalPrice(BigDecimal totalPrice) {
		if (totalPrice == null || totalPrice.signum() <= 0) {
			throw new IllegalArgumentException("주문 총액은 0보다 커야 합니다. 입력 총액: " + totalPrice);
		}
		if (totalPrice.compareTo(MAX_TOTAL_PRICE) > 0) {
			throw new IllegalArgumentException("주문 총액은 " + MAX_TOTAL_PRICE + "을 넘을 수 없습니다. 입력 총액: " + totalPrice);
		}
	}

}
