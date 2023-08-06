package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerFirst {
    public static void main(String[] args) {
        //define static data
        String kafkaUrl = "127.0.0.1:9092";
        String topic = "topic1" ;

        //create producer prop
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , kafkaUrl );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create record
        ProducerRecord<String,String> record = new ProducerRecord<>(topic , "hi baby1 ...");

        //send record via producer
        producer.send(record);

        //flush and close
        producer.flush();
        producer.close();

    }
}
