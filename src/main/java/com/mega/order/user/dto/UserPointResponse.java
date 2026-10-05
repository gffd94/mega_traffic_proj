package com.mega.order.user.dto;

import java.math.BigDecimal;

public record UserPointResponse(
		Long userId,
		BigDecimal pointBalance
) {
}
