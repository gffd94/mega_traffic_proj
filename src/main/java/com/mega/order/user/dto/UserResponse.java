package com.mega.order.user.dto;

import java.math.BigDecimal;

public record UserResponse(
		Long id,
		String name,
		BigDecimal pointBalance
) {
}
