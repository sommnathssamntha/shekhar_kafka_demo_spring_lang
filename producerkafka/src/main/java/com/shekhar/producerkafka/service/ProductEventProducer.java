package com.shekhar.producerkafka.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.shekhar.producerkafka.model.CreateProductRestModel;

@Service
public class ProductEventProducer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final KafkaTemplate<String, CreateProductRestModel> kafkaTemplate;

    @Value("${app.kafka.product-created-topic}")
    private String topicName;

    public ProductEventProducer(KafkaTemplate<String, CreateProductRestModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendProductCreatedEvent(CreateProductRestModel model) {

        logger.info("Producer: Sending product created event for ID: {}", model.productId());

        // create header for tracing
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("correlationId", java.util.UUID.randomUUID().toString().getBytes()));
        headers.add(new RecordHeader("source", "product-service".getBytes()));

        kafkaTemplate.send(
                new ProducerRecord<>(topicName, null, System.currentTimeMillis(), model.productId(), model, headers))
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        logger.info("✅ Publishes to partition : {} with offset : {}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        logger.error("❌ Failed to publish event for productId : {} with error : {}",
                                model.productId(), ex.getMessage());
                    }
                });

    }

}
