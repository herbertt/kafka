package com.file.batch.kafka.listener;


import com.file.batch.kafka.service.StoreMessagesService;

import com.file.batch.kafka.util.FileUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;

@Component
public class MessageListener {

    private final Logger log = LoggerFactory.getLogger(MessageListener.class);

    @Value("${topic.name.consumer}")
    private String topicName;

    @Value("${time.interval}")
    private int timeInterval;

    @Value("${message.number}")
    private int messageNumber;

    private boolean firstTime = true;

    private Date d1 = null;

    private int fileNumber = 1;

    @Autowired
    private StoreMessagesService storeMessages;

    private List<String> receivedMessages = new ArrayList<>();

    @KafkaListener(id = "${spring.kafka.consumer.group-id}", topics = "${topic.name.consumer}", batch = "true")
    public void listen(ConsumerRecords<String, String> records) throws InterruptedException, IOException {
        log.info("Number of elements in the records: {}", records.count());
        if(firstTime){
            firstTime = false;
            d1 = new Date();
        }

        for(ConsumerRecord<String, String> record: records){
            receivedMessages.add(record.value());
            if(fileNumber == messageNumber || verifyTime(d1)){
                storeMessages.save(receivedMessages);
                fileNumber=0;
                receivedMessages.clear();
                d1 = new Date();
            }
            fileNumber++;
        }
        FileUtil.addFilesToZipArchive();

    }
    private boolean verifyTime(Date d1){
        Date d2 = new Date();
        // Get msec from each, and subtract.
        long diff = d2.getTime() - d1.getTime();
        long diffSeconds = diff / 1000 % 60;
        if (diffSeconds >= timeInterval) {
            return true;
        } else {
            return false;
        }
    }

    public List<String> getReceivedMessages() {
        return receivedMessages;
    }
}

