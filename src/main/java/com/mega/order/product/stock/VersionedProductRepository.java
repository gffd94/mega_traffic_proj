package com.mega.order.product.stock;

import org.springframework.data.jpa.repository.JpaRepository;

public interface VersionedProductRepository extends JpaRepository<VersionedProduct, Long> {
}
