package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {


        // Now we connect to our Conduktor PlayGround cluster

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6y7qVcvOyFweu5GfKxlzqz\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2eTdxVmN2T3lGd2V1NUdmS3hsenF6Iiwib3JnYW5pemF0aW9uSWQiOjczOTA1LCJ1c2VySWQiOjg1OTU4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI3NDhkNjdjNi1hNmExLTRiOTktOTM2NS05MWE1MTJhYjEwZDUifX0.-JgXhDjE61NVxmLjDLTjeJNDYYQddHnx7alVUP2JDfs\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());


        // Create a producer

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // Create a Producer Record

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "key: " + i;
                String value = "Hello World: " + i;


                ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);

                // send data
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // This is gonna execute every time of record insertion
                        if (exception == null) {
                            LOGGER.info(
                                     key + " | " + "Partition: " + metadata.partition()
                            );
                        } else {
                            LOGGER.error(exception.toString());
                        }
                    }
                });
            }
            Thread.sleep(500);
        }

        // tell the producer to send all data and block until done
        kafkaProducer.flush();

        // flush and close the producer
        kafkaProducer.close();

    }
}
