package Streaming;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;


public class Processor1 implements Processor<String,Long>{

    private ProcessorContext processorContext;
    private KeyValueStore<String,Long> keyValueStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context){

        // keep the process context locally to schedule punctuate function and commits
        this.processorContext = context;

        this.processorContext.schedule(1000);

        this.keyValueStore = (KeyValueStore<String,Long>) context.getStateStore("Counts");

    }

    @Override
    public void process(String s, Long s2) {

        System.out.println(s);
        System.out.println(Long.toString(s2));
    }


    //This method will be called as a way to do something periodically
    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Long> iter = this.keyValueStore.all();

        while (iter.hasNext()) {
            KeyValue<String, Long> entry = iter.next();

            //forward the calculated values downstream
            processorContext.forward(entry.key, entry.value.toString());
        }

        iter.close();
        // commit the current processing progress
        processorContext  .commit();
    }

    @Override
    public void close(){

    }

    public static void main(String[] args){
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.addSource("SOURCE","test_topic").
                addProcessor("PROCESS1",()->new Processor1()).
                addSink("SINK1","output_test_topic","PROCESS1").
                build(1);
        Properties settings = new Properties();
// Example of a "normal" setting for Kafka Streams
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Customize the Kafka consumer settings
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);

// Customize a common client setting for both consumer and producer
        settings.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, 100L);
        // Customize different values for consumer and producer
        settings.put("consumer." + ConsumerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
        settings.put("producer." + ProducerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
// Alternatively, you can use
        settings.put(StreamsConfig.consumerPrefix(ConsumerConfig.RECEIVE_BUFFER_CONFIG), 1024 * 1024);
        settings.put(StreamsConfig.producerPrefix(ProducerConfig.RECEIVE_BUFFER_CONFIG), 64 * 1024);
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"test_topic_app");
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder,settings);
        kafkaStreams.start();
        System.out.println("Process Running.");
    }
}
