package com.shekhar.producerkafka.service;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.shekhar.producerkafka.model.CreateProductRestModel;

@Service
public class ProductServiceImpl implements ProductService {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String createProduct(CreateProductRestModel productRestModel) {
        String correlationId = UUID.randomUUID().toString();

        logger.info("Service: Creating product with ID: {} and CorrelationId: {}",
                productRestModel.productId(), correlationId);
        return productRestModel.productId();
    }

}
