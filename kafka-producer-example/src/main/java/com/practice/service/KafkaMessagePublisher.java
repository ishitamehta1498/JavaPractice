package com.practice.service;

import com.practice.dto.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    @Autowired
    private KafkaTemplate<String,Object> template;

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<String, Object>> future = template.send("topic4", message);
        future.whenComplete((result,ex)->{
            if(ex==null){
                System.out.println("Send message=["+message+"] with offset=["+result.getRecordMetadata().offset()+"]");
            }else{
                System.out.println("Unable to send message=["+message+"] due to : "+ex.getMessage());
            }
        });
    }

    public void sendEventsToTopic(Consumer consumer){
        try{
            CompletableFuture<SendResult<String, Object>> future = template.send("topic5", consumer);
            future.whenComplete((result,ex)->{
                if(ex==null){
                    System.out.println("Send message=["+consumer.toString()+"] with offset=["+result.getRecordMetadata().offset()+"]");
                }else{
                    System.out.println("Unable to send message=["+consumer.toString()+"] due to : "+ex.getMessage());
                }
            });
        }catch (Exception ex){
            System.out.println("Error: "+ex.getMessage());
        }

    }
}
