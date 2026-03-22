package com.shekhar.producerkafka.rest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.shekhar.producerkafka.model.CreateProductRestModel;
import com.shekhar.producerkafka.service.ProductEventProducer;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/v1/products")
@Tag(name = "Product Controller", description = "Endpoints for managing product creation events in the Kafka cluster")
public class ProductController {
    private final ProductEventProducer producer;

    public ProductController(ProductEventProducer producer) {
        this.producer = producer;
    }

    @Operation(summary = "Create a new product event", description = "Publishes a validated product event to the 3-node Kafka cluster using acks=all for high durability.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Accepted - Event successfully published to Kafka"),
            @ApiResponse(responseCode = "400", description = "Bad Request - Validation failed for the product model"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error - Kafka cluster is unreachable")
    })
    @PostMapping
    public ResponseEntity<String> createProduct(@Valid @RequestBody CreateProductRestModel productRequest) {
        // Use the "productId" from the JSON as the Kafka KEY
        String productId = productRequest.productId();
        // Send to Kafka
        producer.sendProductCreatedEvent(productRequest);

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body("Product event accepted for ID: " + productId);
    }
}
