package com.example.kafkatest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@Slf4j
@RestController
public class KafkaTestApplication {

    @Autowired
    KafkaTemplate template;

    public static void main(String[] args) {
        SpringApplication.run(KafkaTestApplication.class, args);
    }


    /**
     * 重试3次
     * @param record
     */
    @RetryableTopic(attempts = "3")
//    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 20000, multiplier = 2, maxDelay = 200000))
    @KafkaListener(topics = {"test"})
    public void handleMessage(ConsumerRecord<?, ?> record) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (!kafkaMessage.isPresent()) {
            return;
        }
        String message = kafkaMessage.get().toString();
        log.info("topic---: {},{}", record.topic(), record.headers());
        log.warn("message---:{}", message);
        throw new RuntimeException("kafka exception");
    }

    /**
     * 重试放到死信队列中
     * @param record
     */
    @KafkaListener(topics = {"test-dlt"})  // 这句话不起作用
    @DltHandler
    public void handleMessageDlt(ConsumerRecord<?, ?> record) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (!kafkaMessage.isPresent()) {
            return;
        }
        String message = kafkaMessage.get().toString();
        log.info("topic---Dlt: {},{}", record.topic(), record.headers());
        log.warn("message---Dlt:{}", message);
    }

    /**
     * topic2:test2
     * @param record
     */
    @KafkaListener(topics = {"test2"})
    public void handleMessage2(ConsumerRecord<?, ?> record) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (!kafkaMessage.isPresent()) {
            return;
        }
        String message = kafkaMessage.get().toString();
        log.info("topic---2: {},{}", record.topic(), record.headers());
        log.warn("message---2:{}", message);
    }


    @GetMapping("test")
    @Transactional
    public String test(@RequestParam String msg) throws ExecutionException, InterruptedException {
        // 发送事务消息
        ListenableFuture send = template.send("test", msg);
        ListenableFuture send2 = template.send("test2", msg);
        return send.get().toString() + send2.get().toString();
    }
}
