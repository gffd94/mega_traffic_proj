package com.mega.order.user.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record UserCreateRequest(
		@NotBlank @Size(max = 50) String name
) {
}
