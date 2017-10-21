//import util.properties packages
import java.util.Properties;
import java.util.Scanner;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;



//Create java class named “SimpleProducer”
public class SimpleProducer {

    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";

    public static void main(String[] args) throws Exception{

        // Check arguments length value
        if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }
        System.out.println("Topic passed");

        //Assign topicName to string variable
        String topicName = args[0].toString();

        System.out.println(topicName);

        // create instance for properties to access producer configs
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        Scanner s = new Scanner(System.in);

        while(true){
            String input = s.nextLine();
            if(input=="exit"){
                break;
            }
            producer.send(new ProducerRecord<>(topicName,Integer.toString(1),input));
        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}