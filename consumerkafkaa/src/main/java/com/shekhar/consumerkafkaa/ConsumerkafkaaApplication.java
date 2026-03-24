package com.shekhar.consumerkafkaa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ConsumerkafkaaApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerkafkaaApplication.class, args);
	}

}
