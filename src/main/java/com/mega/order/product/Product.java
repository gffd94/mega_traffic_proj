package com.mega.order.product;

import com.mega.order.global.BaseTimeEntity;
import com.mega.order.global.exception.InsufficientStockException;
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
@Table(name = "products")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Product extends BaseTimeEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, length = 100)
	private String name;

	@Column(nullable = false, precision = 12, scale = 2)
	private BigDecimal price;

	@Column(nullable = false)
	private int stockQuantity;

	@Builder
	private Product(String name, BigDecimal price, int stockQuantity) {
		validateName(name);
		validatePrice(price);
		validateStockQuantity(stockQuantity);
		this.name = name;
		this.price = price;
		this.stockQuantity = stockQuantity;
	}

	public void decreaseStock(int quantity) {
		validateQuantity(quantity);
		if (quantity > this.stockQuantity) {
			throw new InsufficientStockException(quantity, this.stockQuantity);
		}
		this.stockQuantity -= quantity;
	}

	public void increaseStock(int quantity) {
		validateQuantity(quantity);
		this.stockQuantity += quantity;
	}

	private void validateName(String name) {
		if (name == null || name.isBlank()) {
			throw new IllegalArgumentException("상품명은 필수입니다.");
		}
	}

	private void validatePrice(BigDecimal price) {
		if (price == null || price.signum() <= 0) {
			throw new IllegalArgumentException("가격은 0보다 커야 합니다. 입력 가격: " + price);
		}
	}

	private void validateStockQuantity(int stockQuantity) {
		if (stockQuantity < 0) {
			throw new IllegalArgumentException("재고 수량은 0 이상이어야 합니다. 입력 수량: " + stockQuantity);
		}
	}

	private void validateQuantity(int quantity) {
		if (quantity <= 0) {
			throw new IllegalArgumentException("수량은 0보다 커야 합니다. 입력 수량: " + quantity);
		}
	}

}
