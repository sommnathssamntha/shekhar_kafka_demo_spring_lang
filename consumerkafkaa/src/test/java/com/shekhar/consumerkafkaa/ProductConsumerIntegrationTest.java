package com.shekhar.consumerkafkaa;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import com.shekhar.consumerkafkaa.model.CreateProductRestModel;
import com.shekhar.consumerkafkaa.persistence.ProductRepository;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = { "product-created-events-topic" })
@ActiveProfiles("test")
@TestPropertySource(properties = {
        // 🛡️ Force the Consumer to match the Test Producer's JSON
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.group-id=test-group-id",
        "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer",
        "spring.kafka.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JsonDeserializer",
        "spring.kafka.properties.spring.json.trusted.packages=*",
        "spring.kafka.properties.spring.json.use.type.headers=false",
        "spring.kafka.properties.spring.json.value.default.type=com.shekhar.consumerkafkaa.model.CreateProductRestModel"
})
class ProductConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ProductRepository repository;

    @Test
    void testKafkaToH2_Integration() {
        // 1. CREATE A LOCAL, JSON-CAPABLE TEMPLATE
        Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, Object> pf = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<String, Object> manualTemplate = new KafkaTemplate<>(pf);

        // 2. DATA PREP
        String productId = "TEST-ID-123";
        var event = new CreateProductRestModel(productId, "Test", BigDecimal.valueOf(100.0), 1, 1L);

        // 3. SEND (Using the MANUAL template, not the Autowired one)
        manualTemplate.send("product-created-events-topic", productId, event);

        // 4. VERIFY
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertTrue(repository.existsById(productId), "Failed: Product not saved in H2!");
                });
    }

}
