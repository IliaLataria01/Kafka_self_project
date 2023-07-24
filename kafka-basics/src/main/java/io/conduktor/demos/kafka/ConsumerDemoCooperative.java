package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {

        String groupId = "my-java-application";
        String topic = "demo_java";

        // Now we connect to our Conduktor PlayGround cluster

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6y7qVcvOyFweu5GfKxlzqz\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2eTdxVmN2T3lGd2V1NUdmS3hsenF6Iiwib3JnYW5pemF0aW9uSWQiOjczOTA1LCJ1c2VySWQiOjg1OTU4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI3NDhkNjdjNi1hNmExLTRiOTktOTM2NS05MWE1MTJhYjEwZDUifX0.-JgXhDjE61NVxmLjDLTjeJNDYYQddHnx7alVUP2JDfs\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.reset","earliest");

        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

//        properties.setProperty("group.instance.id","") Strategy for static assignments


        KafkaConsumer<String,String > kafkaConsumer = new KafkaConsumer<>(properties);


        // Get a reference to the mian thread

        final Thread mainThread = Thread.currentThread();

        // Adding ShutDown Hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("Detected a shutdown, let's exit by calling consumer.wakeUp()...");

                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // Pull data
        try {

            kafkaConsumer.subscribe(Arrays.asList(topic));

            while (true) {

                ConsumerRecords<String,String > consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));


                for (ConsumerRecord<String,String> record : consumerRecords) {
                    LOGGER.info("Record offset: " + record.offset() + " - Record partition: " +  record.partition());
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            LOGGER.info("Unexpected exception,",e);
        } finally {
            kafkaConsumer.close(); // This also commits offsets
            LOGGER.info("Consumer is now gracefully shutdown");
        }

    }
}
