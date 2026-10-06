package com.mega.order.product;

import com.mega.order.global.exception.ProductNotFoundException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProductReader {

	private final ProductRepository productRepository;

	public List<ProductInfo> getProductInfos(List<Long> productIds) {
		List<Product> products = productRepository.findAllById(productIds);

		Set<Long> foundIds = products.stream()
				.map(Product::getId)
				.collect(Collectors.toSet());
		List<Long> missingIds = productIds.stream()
				.filter(id -> !foundIds.contains(id))
				.distinct()
				.toList();
		if (!missingIds.isEmpty()) {
			throw new ProductNotFoundException(missingIds);
		}

		return products.stream()
				.map(product -> new ProductInfo(product.getId(), product.getPrice()))
				.toList();
	}

}
