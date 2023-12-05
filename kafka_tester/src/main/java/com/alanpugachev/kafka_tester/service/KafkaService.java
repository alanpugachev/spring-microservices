package com.alanpugachev.kafka_tester.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaService {

    private final KafkaTemplate<Long, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public KafkaService(KafkaTemplate<Long, Object> kafkaTemplate,
                        ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(initialDelay = 10000, fixedDelay = 5000)
    public void produce() {
        log.info("<-- sending {}", writeValueAsString("aaa"));
        kafkaTemplate.send("message", "aaa");
    }

    private String writeValueAsString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException("Writing value to JSON failed: " + object.toString());
        }
    }
}
