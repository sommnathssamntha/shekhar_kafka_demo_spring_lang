package com.shekhar.producerkafka.model;

import java.math.BigDecimal;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record CreateProductRestModel(
        @NotBlank(message = "productId is mandatory") String productId,

        @NotBlank(message = "name is mandatory") String name,

        @NotNull(message = "price is mandatory") @Positive(message = "price must be positive") BigDecimal price,

        @NotNull(message = "quantity is mandatory") @Positive(message = "quantity must be positive") Integer quantity) {
}
