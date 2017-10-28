package Streaming;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class stream_producer {

    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static final void main(String[] args) throws Exception{
        String topic = "test_topic";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"StreamProducerApplication");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        Producer<String,String> producer = new KafkaProducer<>(props);
        System.out.println("Stream Producer Running.");

        Scanner scanner = new Scanner(System.in);
        int i=0;
        while(true){
            String input = scanner.nextLine();
            if(input == "exit")
                break;
            producer.send(new ProducerRecord(topic,input,input));
            i+=1;
        }
        System.out.println("Total Messages sent:"+Integer.toString(i));
        producer.close();
        
    }

}
