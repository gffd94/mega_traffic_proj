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

	private void validateQuantity(int quantity) {
		if (quantity <= 0) {
			throw new IllegalArgumentException("수량은 0보다 커야 합니다. 입력 수량: " + quantity);
		}
	}

}
