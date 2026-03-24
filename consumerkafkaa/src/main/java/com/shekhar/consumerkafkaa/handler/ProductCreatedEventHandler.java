package com.shekhar.consumerkafkaa.handler;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
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
    public void handleProductCreatedEvent(
            @Payload CreateProductRestModel event,
            @Header("correlationid") String correlationId,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment acknowledgment) {

        MDC.put("correlationId", correlationId);
        var productId = event.productId();
        logger.info("📥 Received: {} | Version: {} | Correlation: {}", productId, event.version(), correlationId);

        try {
            // 1. FETCH STATE
            Optional<ProductEntity> existing = productRepository.findById(productId);

            // 2. VERSION GUARD (If exists, check version)
            if (existing.isPresent() && event.version() <= existing.get().getVersion()) {
                logger.warn("🕒 Stale Event: ID {} v{} <= DB v{}. Skipping.",
                        productId, event.version(), existing.get().getVersion());
                acknowledgment.acknowledge(); // IMPORTANT
                return;
            }

            // 3. PERSISTENCE (Handles both NEW products and NEWER versions)
            productRepository.save(ProductEntity.builder()
                    .productId(productId)
                    .name(event.name())
                    .price(event.price())
                    .quantity(event.quantity())
                    .version(event.version())
                    .build());

            logger.info("✅ Product {} (v{}) persisted to DB.", productId, event.version());
            acknowledgment.acknowledge(); // IMPORTANT

        } catch (DataAccessException ex) {
            // Retryable: DB is down. Throwing this triggers @RetryableTopic (NO ACK)
            throw new RetryableException("DB Temporary Issue", ex);
        } catch (Exception ex) {
            // Fatal: Log and move to DLT
            logger.error("❌ Fatal error for {}: {}", productId, ex.getMessage());
            acknowledgment.acknowledge(); // IMPORTANT: Moves to DLT
        } finally {
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
