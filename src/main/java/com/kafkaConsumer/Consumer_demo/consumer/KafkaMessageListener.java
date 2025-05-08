package com.kafkaConsumer.Consumer_demo.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaConsumer.Consumer_demo.dto.Customer;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class KafkaMessageListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "KafkaPractice-3",groupId = "consumer-group-1")
    public void consume1(String message,@Header(KafkaHeaders.OFFSET) String offset){

        System.out.println("consumer consume the event {} " + message);
    }

    @RetryableTopic(attempts = "3")
    @KafkaListener(topics = "JSONTopic1",groupId = "consumer-group-2")
    public void consume2(String message,@Header(KafkaHeaders.OFFSET) String offset) throws JsonProcessingException {
        try {
            Customer customer = objectMapper.readValue(message, Customer.class);
            if (Objects.equals(customer.getName(), "Alpish")) {
                throw new RuntimeException("Exception here");
            }
            System.out.println("consumer consume the event {} " + customer.toString());
        } catch (JsonProcessingException e) {
           e.printStackTrace();
        }
    }

    @DltHandler
    public void dltConsumer(String message,@Header(KafkaHeaders.OFFSET) String offset) {
        System.out.println("Received in DLT topic "+message);
    }
}
