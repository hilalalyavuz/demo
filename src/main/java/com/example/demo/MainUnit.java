package com.example.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MainUnit {

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    // Threadpool of consumers
    private ExecutorService executor;
    private List<String> string_arr = new ArrayList<String>();

    public MainUnit(String brokers, String groupId, String topic) {
        Properties prop = createConsumerConfig(brokers, groupId);
        this.consumer = new KafkaConsumer<>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    /**
     * Creates a {@link ThreadPoolExecutor} with a given number of threads to consume the messages
     * from the broker.
     *
     * @param numberOfThreads The number of threads will be used to consume the message
     */
    public void execute(int numberOfThreads) {

        // Initialize a ThreadPool with size = 2 and use the BlockingQueue with size =1000 to
        // hold submitted tasks.
        executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (final ConsumerRecord record : records) {
                executor.submit(new MainUnitThreadHandler(record));
                string_arr.add((String) record.value());
                if (string_arr.size() == 2){
                    findTarget(string_arr);
                }
            }
        }
    }

    private static void findTarget(List<String> str_arr){
        double targetX = 0;
        double targetY = 0;

        double[] firstSensor = Arrays.stream(str_arr.get(0).split(","))
                .mapToDouble(Double::parseDouble)
                .toArray();
        double[] secondSensor = Arrays.stream(str_arr.get(1).split(","))
                .mapToDouble(Double::parseDouble)
                .toArray();

        double x1 = firstSensor[0];
        double y1 = firstSensor[1];
        double theta1 = firstSensor[2];
        double m1 = Math.tan(Math.toRadians(theta1));
        double b1 = y1 - m1 * x1;


        double x2 = secondSensor[0];
        double y2 = secondSensor[1];
        double theta2 = secondSensor[2];
        double m2 = Math.tan(Math.toRadians(theta2));
        double b2 = y2 - m2 * x2;

        targetX = (b2 - b1) / (m1 - m2);
        targetY = m1 * targetX + b1;

        System.out.println("Target: " + "X: " + targetX + " Y: " + targetY);

    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out
                        .println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

}
