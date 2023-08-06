package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithPartion {
    public static void main(String[] args) {
        //create static data
        String bootStrapUrl = "127.0.0.1:9092";
        String topic = "test1" ;
        String group = "g1";

        //create properties
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapUrl);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        //group
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        //getting data strategy
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ,"earliest");

        // create kafka consumer
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(p);
        //set partition
        TopicPartition tp = new TopicPartition(topic,0);
//        kafkaConsumer.subscribe(Arrays.asList(topic));
        kafkaConsumer.assign(Arrays.asList(tp));
        while (true){
            //make record poll from topic
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(50));
            for(ConsumerRecord<String,String> r : records){
                String msg = "topic : " + r.topic() +
                        " partition : " + r.partition() +
                        " offset : " + r.offset() +
                        " key : " + r.key() +
                        " value : " + r.value();
                System.out.println(msg);
            }
        }


    }
}
