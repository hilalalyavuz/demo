package com.example.demo;

public final class MainUnitMain {

    public static void main(String[] args) {

        String brokers = "localhost:9092";
        String groupId = "group01";
        String topic = "coordinate";
        int numberOfThread = 3;

        if (args != null && args.length > 4) {
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
            numberOfThread = Integer.parseInt(args[3]);
        }

        // Start Sensor Producer Thread
        SensorThread producerThread = new SensorThread(brokers, topic);
        Thread t1 = new Thread(producerThread);
        t1.start();

        // Start group of Main Unit Consumer Thread
        MainUnit consumers = new MainUnit(brokers, groupId, topic);

        consumers.execute(numberOfThread);

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
        consumers.shutdown();
    }
}
