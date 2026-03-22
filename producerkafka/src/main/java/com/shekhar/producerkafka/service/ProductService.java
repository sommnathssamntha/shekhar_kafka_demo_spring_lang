package com.shekhar.producerkafka.service;

import com.shekhar.producerkafka.model.CreateProductRestModel;

public interface ProductService {

    String createProduct(CreateProductRestModel productRestModel) throws Exception;

}
