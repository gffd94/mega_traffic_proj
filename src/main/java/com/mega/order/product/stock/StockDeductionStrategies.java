package com.mega.order.product.stock;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

@Component
public class StockDeductionStrategies {

	private final Map<String, StockDeductionStrategy> strategiesByName;

	public StockDeductionStrategies(List<StockDeductionStrategy> strategies) {
		this.strategiesByName = strategies.stream()
				.collect(Collectors.toUnmodifiableMap(StockDeductionStrategy::name, Function.identity()));
	}

	public StockDeductionStrategy get(String name) {
		StockDeductionStrategy strategy = strategiesByName.get(name);
		if (strategy == null) {
			throw new IllegalArgumentException(
					"지원하지 않는 재고 차감 방식입니다. 입력: " + name + ", 지원: " + strategiesByName.keySet());
		}
		return strategy;
	}

}
