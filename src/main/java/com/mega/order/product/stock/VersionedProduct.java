package com.mega.order.product.stock;

import com.mega.order.global.exception.InsufficientStockException;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 낙관적 락 실험 전용 엔티티. products 테이블을 Product와 함께 매핑한다.
 * Product에 @Version을 붙이면 none 방식까지 충돌을 감지해 비교 기준이 사라지므로 따로 둔다.
 * 생성자를 열지 않아 INSERT는 하지 않고, 조회와 재고 차감만 한다.
 */
@Getter
@Entity
@Table(name = "products")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class VersionedProduct {

	@Id
	private Long id;

	@Column(nullable = false)
	private int stockQuantity;

	// Product로 INSERT되는 행과 기존 행이 0으로 채워지도록 DB 기본값을 둔다.
	@Version
	@Column(nullable = false, columnDefinition = "bigint default 0")
	private Long version;

	public void decreaseStock(int quantity) {
		validateQuantity(quantity);
		if (quantity > this.stockQuantity) {
			throw new InsufficientStockException(quantity, this.stockQuantity);
		}
		this.stockQuantity -= quantity;
	}

	private void validateQuantity(int quantity) {
		if (quantity <= 0) {
			throw new IllegalArgumentException("수량은 0보다 커야 합니다. 입력 수량: " + quantity);
		}
	}

}
