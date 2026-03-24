package com.shekhar.consumerkafkaa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.shekhar.consumerkafkaa.model.CreateProductRestModel;
import com.shekhar.consumerkafkaa.persistence.ProductRepository;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = { "product-created-events-topic" })
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer",
        "spring.kafka.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer",
        "spring.kafka.consumer.properties.spring.json.trusted.packages=*",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.properties.spring.json.use.type.headers=false"
})
class ProductConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ProductRepository repository;

    @Test
    void testKafkaToH2_Integration() {
        // 1. Prepare a Mock Event
        String productId = "TEST-LAPTOP-999";
        CreateProductRestModel event = new CreateProductRestModel(productId, "Test", BigDecimal.valueOf(100.0), 1, 1L);

        // 2. Push to the Embedded Broker
        kafkaTemplate.send("product-created-events-topic", productId, event);

        // 3. AWAITILITY: Industrial way to wait for async processing
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    var result = repository.findById(productId);
                    assertTrue(result.isPresent(), "Record should exist in H2!");
                    assertEquals(1L, result.get().getVersion());
                });
    }

}
