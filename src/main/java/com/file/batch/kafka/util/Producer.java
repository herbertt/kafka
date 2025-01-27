package com.file.batch.kafka.util;

import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class Producer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, String message) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(topic, message).get();
        this.kafkaTemplate.flush();
    }
}
