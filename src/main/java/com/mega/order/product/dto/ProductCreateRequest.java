package com.mega.order.product.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;

public record ProductCreateRequest(
		@NotBlank String name,
		@NotNull @Positive BigDecimal price,
		@NotNull @PositiveOrZero Integer stockQuantity
) {
}
