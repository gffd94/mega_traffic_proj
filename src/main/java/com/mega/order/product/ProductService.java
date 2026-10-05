package com.mega.order.product;

import com.mega.order.product.dto.ProductCreateRequest;
import com.mega.order.product.dto.ProductResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ProductService {

	private final ProductRepository productRepository;
	private final ProductMapper productMapper;

	@Transactional
	public ProductResponse create(ProductCreateRequest request) {
		Product product = productMapper.toEntity(request);
		Product savedProduct = productRepository.save(product);
		return productMapper.toResponse(savedProduct);
	}

}
