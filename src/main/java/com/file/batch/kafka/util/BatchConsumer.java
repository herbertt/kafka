package com.file.batch.kafka.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BatchConsumer {

    private final Logger logger = LoggerFactory.getLogger(BatchConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private List<String> receivedMessages = new ArrayList<>();

    @KafkaListener(id = "batch-listener", topics = "batch_topic", batch = "true", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecords<String, String> records) throws InterruptedException {
        logger.info("Number of elements in the records: {}", records.count());
        records.forEach(record -> receivedMessages.add(record.value()));
        latch.await();
        latch = new CountDownLatch(1);
    }
    public CountDownLatch getLatch() {
        return latch;
    }
    public List<String> getReceivedMessages() {
        return receivedMessages;
    }

}
