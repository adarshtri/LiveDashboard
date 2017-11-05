
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serde;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class StreamsExample {

    public static void main(String[] args){

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<String,String> kStream = kStreamBuilder.stream("test_topic");

        String to_topic_flag = "groupby";


        // Branch Operation

        KStream<String,String>[] kStream_branch = kStream
                .branch(
                        (key,value) -> key.startsWith("C"),
                        (key,value) -> key.startsWith("T"),
                        (key,value) -> true
                );

        if(to_topic_flag=="branch")
            kStream_branch[0].to("a_topic");

        //filter operation

        KStream<String,String>  kStream_filter = kStream.filter(
                //(key,value) -> Integer.parseInt(value.split("\t")[0]) > 14
                (key,value) -> !value.split("\t")[0].equals("NULL")
        );

        if(to_topic_flag=="filter")
            kStream_filter.to("filter_topic");


        // negative filter --> filterNot -- not implemented

        KStream<String,String> kStream_flapmap = kStream.flatMap(
                (key,value) ->{
                    List<KeyValue<String,String>> list = new LinkedList<>();
                    list.add(KeyValue.pair(value,"Hey"));
                    list.add(KeyValue.pair(value,"Hi"));
                    return list;
                }
        );

        if(to_topic_flag=="flatMap")
            kStream_flapmap.to("hey_hi_topic");

        /*
        FlatMap (values only): KStream → KStream
        Takes one record and produces zero, one, or more records,
        while retaining the key of the original record. You can modify the record values and the value type.

        KStream<byte[], String> sentences = ...;
        KStream<byte[], String> words = sentences.flatMapValues(value -> Arrays.asList(value.split("\\s+")));
       */


        //forEach
        if(to_topic_flag=="foreach")
            kStream.foreach((key,value)->System.out.println(key+"===>"+value));


        //map

        KStream<String,String> kStream_map = kStream.map(
                (key,value)->KeyValue.pair(value.toLowerCase(),Integer.toString(value.length()))
        );
        if(to_topic_flag=="map")
            kStream_map.to("topic_map");

        //mapValues function is used to map only values and doesn't allow to change key

        //print

        if(to_topic_flag=="map_print")
            kStream_map.print();


        //selectKey -- transforms key

        //can be use in this case: 1st stream : campaign:id,...... change to id:......
        KStream<String,String> kStream_selectKey = kStream.selectKey(
                (key,value) -> value.split(" ")[0]
        );

        if(to_topic_flag=="selectKey")
            kStream_selectKey.print();

        // table to stream
        // WriteAsText : writes the stream or record to an output file


        KTable<String,Long> kTableGroupKey = kStream.groupByKey().count();
        if(to_topic_flag=="groupbyKey")
            kTableGroupKey.to(Serdes.String(),Serdes.Long(),"groupbyStream");



        KTable<String,Long> kTableGroup = kStream.groupBy(
                (key,value) -> value.toUpperCase()
        ).count();
        if(to_topic_flag=="groupby")
            kTableGroup.to(Serdes.String(),Serdes.Long(),"gorupbyStreamValue");


        //Run the streaming application

        KafkaStreams streams = new KafkaStreams(kStreamBuilder, config);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));





    }
}
