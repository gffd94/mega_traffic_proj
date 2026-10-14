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

	/*
	 * 상태 변경 메서드는 Saga 이벤트로만 호출되고, 이벤트는 큐마다 따로 와서 도착 순서가 바뀌거나 중복될 수 있다.
	 * 그래서 이미 끝난 주문(COMPLETED/CANCELED)이거나 이미 목표 상태인 경우는 잘못된 요청이 아니라
	 * "늦게 온 이벤트"로 보고, 예외 대신 false를 돌려준다(상태는 그대로). 반영했으면 true.
	 * 예외를 던지면 컨슈머가 재시도해도 결과가 같아 메시지가 결국 버려지기만 한다.
	 */

	public boolean reserveStock() {
		if (isFinished() || this.status == OrderStatus.STOCK_RESERVED) {
			return false;
		}
		this.status = OrderStatus.STOCK_RESERVED;
		return true;
	}

	// PaymentCompleted가 StockReserved보다 먼저 올 수 있으므로 PENDING에서도 바로 확정한다
	public boolean complete() {
		if (isFinished()) {
			return false;
		}
		this.status = OrderStatus.COMPLETED;
		return true;
	}

	public boolean cancel(CancelReason reason) {
		if (reason == null) {
			throw new IllegalArgumentException("취소 사유는 필수입니다.");
		}
		if (isFinished()) {
			return false;
		}
		this.status = OrderStatus.CANCELED;
		this.cancelReason = reason;
		return true;
	}

	public boolean isFinished() {
		return this.status == OrderStatus.COMPLETED || this.status == OrderStatus.CANCELED;
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
