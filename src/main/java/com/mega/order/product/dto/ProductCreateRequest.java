package com.mega.order.product.dto;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record ProductCreateRequest(
		@NotBlank @Size(max = 100) String name,
		@NotNull @Positive @Digits(integer = 10, fraction = 2) BigDecimal price,
		@NotNull @PositiveOrZero Integer stockQuantity
) {
}
