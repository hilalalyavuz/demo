package com.example.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SensorThread implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private double maxPointX = 1000;
    private double maxPointY = 1000;
    private int MAX_THREAD = 2;

    public SensorThread(String brokers, String topic) {
        Properties prop = createProducerConfig(brokers);
        this.producer = new KafkaProducer<String, String>(prop);
        this.topic = topic;
    }

    private static Properties createProducerConfig(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Override
    public void run() {
        double targetX = -maxPointX + (Math.random() * ((maxPointX - (-maxPointX)) + 1));
        double targetY = -maxPointY + (Math.random() * ((maxPointY - (-maxPointY)) + 1));
        for (int i = 0; i < MAX_THREAD; i++) {
            double pointX = -maxPointX + (Math.random() * ((maxPointX - (-maxPointX)) + 1));
            double pointY = -maxPointY + (Math.random() * ((maxPointY - (-maxPointY)) + 1));
            double angle = Math.toDegrees(Math.atan2(targetY - pointY, targetX - pointX));
            if (angle < 0){
                angle += 360;
            }
            String msg = pointX + "," + pointY + "," + angle;
            producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out.println("Sent:" + msg + ", Offset: " + metadata.offset());
                }
            });
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        // closes producer
        producer.close();

    }
}
