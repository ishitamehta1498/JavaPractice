package com.practice.controller;

import com.practice.dto.Consumer;
import com.practice.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/publisher")
public class EventController {
    @Autowired
    private KafkaMessagePublisher publisher;
    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable("message") String message){
        try{
            for(int i=1;i<=10000;i++){
                publisher.sendMessageToTopic(message+":"+i);
            }
            return ResponseEntity.ok("message published successfully");
        }catch (Exception ex){
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/publish")
    public void sendEvents(@RequestBody Consumer consumer){
        publisher.sendEventsToTopic(consumer);
    }

}
