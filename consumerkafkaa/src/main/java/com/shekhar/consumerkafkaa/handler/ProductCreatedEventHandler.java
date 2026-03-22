package com.shekhar.consumerkafkaa.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.shekhar.consumerkafkaa.exception.RetryableException;
import com.shekhar.consumerkafkaa.model.CreateProductRestModel;
import com.shekhar.consumerkafkaa.persistence.ProductEntity;
import com.shekhar.consumerkafkaa.persistence.ProductRepository;

@Component

public class ProductCreatedEventHandler {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ProductRepository productRepository;

    public ProductCreatedEventHandler(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @RetryableTopic(attempts = "3", backOff = @BackOff(delay = 2000, multiplier = 2), include = RetryableException.class, dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = "${app.kafka.product-created-topic}")
    public void handleProductCreatedEvent(@Payload CreateProductRestModel event,
            @Header("correlationid") String correlationId,
            @Header("key") String key) {
        MDC.put("correlationId", correlationId);
        var productId = event.productId();
        logger.info("Received Product Created Event: {} | Correlation: {} | key: {}", productId, correlationId, key);

        // Idempotency Guard
        if (productRepository.existsById(productId)) {
            logger.warn("Product with id {} already exists. Skipping processing.", productId);
            return;
        }

        try {
            productRepository.save(ProductEntity.builder()
                    .productId(productId)
                    .name(event.name())
                    .price(event.price())
                    .quantity(event.quantity())
                    .build());
            logger.info("✅ Product {} successfully persisted to DB.", event.productId());
        } catch (DataAccessException ex) {
            // Sonar Fix: Pass 'ex' as the cause to preserve the stacktrace
            throw new RetryableException("DB Temporary Issue", ex);
        } catch (Exception ex) {
            // Fatal error: Don't retry, let it go to DLT
            logger.error("❌ Failed to process Product Created Event for productId {}. Error: {}",
                    productId, ex.getMessage());
        } finally {
            // 2. CRITICAL: Always clear MDC so the next message doesn't
            // accidentally use the old Correlation ID
            MDC.clear();
        }
    }

    @DltHandler
    public void handleDLT(CreateProductRestModel event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.error("💀 Message Hospital: Product {} landed in DLT from topic {}",
                event.productId(), topic);

    }
}
