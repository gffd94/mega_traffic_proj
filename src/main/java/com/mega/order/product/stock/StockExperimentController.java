package com.mega.order.product.stock;

import com.mega.order.product.stock.dto.StockDeductionRequest;
import com.mega.order.product.stock.dto.StockDeductionResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/experiments/stock")
@RequiredArgsConstructor
public class StockExperimentController {

	private final StockDeductionStrategies stockDeductionStrategies;

	@PostMapping("/{strategy}")
	public ResponseEntity<StockDeductionResponse> deduct(@PathVariable String strategy,
			@Valid @RequestBody StockDeductionRequest request) {
		StockDeductionStrategy stockDeductionStrategy = stockDeductionStrategies.get(strategy);
		stockDeductionStrategy.deduct(request.productId(), request.quantity());
		return ResponseEntity.ok(new StockDeductionResponse(request.productId(), stockDeductionStrategy.name()));
	}

}
