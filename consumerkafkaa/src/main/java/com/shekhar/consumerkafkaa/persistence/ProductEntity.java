package com.shekhar.consumerkafkaa.persistence;

import java.math.BigDecimal;

import com.shekhar.consumerkafkaa.model.CreateProductRestModel;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "products")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProductEntity {

    @Id
    private String productId; // Using Kafka Key as Primary Key ensures Idempotency
    private String name;
    private BigDecimal price;
    private Integer quantity;

    // Versioning for Out-of-Order protection
    private Long version;

}
