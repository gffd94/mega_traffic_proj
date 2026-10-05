package com.mega.order.product;

import com.mega.order.product.dto.ProductCreateRequest;
import com.mega.order.product.dto.ProductResponse;
import org.springframework.stereotype.Component;

@Component
public class ProductMapper {

	public Product toEntity(ProductCreateRequest request) {
		return Product.builder()
				.name(request.name())
				.price(request.price())
				.stockQuantity(request.stockQuantity())
				.build();
	}

	public ProductResponse toResponse(Product product) {
		return new ProductResponse(
				product.getId(),
				product.getName(),
				product.getPrice(),
				product.getStockQuantity()
		);
	}

}
