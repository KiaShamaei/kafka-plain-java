package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerMultipleWithKey {
    public static void main(String[] args) {
        //define static data
        String kafkaUrl = "127.0.0.1:9092";
        String topic = "test1" ;

        //create producer prop
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , kafkaUrl );
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        for(int i = 0 ; i < 10 ; i++ ){
            // same key
            String key = "k1";
            //create record
            ProducerRecord<String,String> record = new ProducerRecord<>(topic ,key ,"msg no : " + i);

            //send record via producer and get callBack
            producer.send(record , (m,e)->{
                String log = "topic : " + m.topic()
                        + " partition : " + m.partition()
                        + " key : " + key
                        + " offset : " + m.offset();
                System.out.println(log);
            });

            //flush and close
            producer.flush();
        }

        producer.close();

    }
}
