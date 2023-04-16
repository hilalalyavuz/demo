package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class MainUnitThreadHandler implements Runnable {

    private ConsumerRecord consumerRecord;

    public MainUnitThreadHandler(ConsumerRecord consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    public void run() {
        System.out.println("Process: " + consumerRecord.value() + ", Offset: " + consumerRecord.offset()
                + ", By ThreadID: " + Thread.currentThread().getId());
    }
}