package pjatk.tpo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import javax.swing.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) {
        EmbeddedKafkaBroker embeddedKafkaBroker = new EmbeddedKafkaBroker(1).
                kafkaPorts(9092);

        embeddedKafkaBroker.afterPropertiesSet();

        GlobalMessageConsumer globalMessageConsumer = new GlobalMessageConsumer();
        globalMessageConsumer.start();
        //toDo u wanna make sure that u have all the topics and u inform if smth new change
        List<ChatWindow> readers= new ArrayList<>();
        SwingUtilities.invokeLater( () ->{
                    ChatWindow chatWindow = new ChatWindow("chat", "Kinga");
                    readers.add(chatWindow);
                });
        SwingUtilities.invokeLater( () -> {
            ChatWindow chatWindow = new ChatWindow("chat", "Jak00b");
            readers.add(chatWindow);
        });



    }
}
class GlobalMessageConsumer {
    private KafkaConsumer<String, String> kafkaConsumer;
    private Set<String> topics= new HashSet<>();
    private List<ChatWindow> readers;
    public GlobalMessageConsumer() {
        this.kafkaConsumer = new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ConsumerConfig.GROUP_ID_CONFIG, "globalConsumerGroup",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                )
        );

    }

    public void start() {
        new Thread(() -> {
            while (true) {
                kafkaConsumer.subscribe(Pattern.compile(".*")); // Nasłuchiwanie na wszystkie tematy
                kafkaConsumer.poll(Duration.ofMillis(100)).forEach(this::processMessage);
            }
        }).start();
    }

    private void processMessage(ConsumerRecord<String, String> record) {
        System.out.println("Received message on topic " + record.topic() + ": " + record.value());

           if(topics.add(record.topic())){
               MessageProducer.send(new ProducerRecord<>("metadata",record.topic()));
           }

        // Tutaj można wykonać dodatkowe operacje za każdym razem, gdy przychodzi wiadomość
    }
}
class MessageProducer{
    private static KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(
            Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
            )
    );
    public static void send(ProducerRecord<String,String> producerRecord) {
        kafkaProducer.send(producerRecord);
    }
}
class MessageConsumer{
    KafkaConsumer<String,String> kafkaConsumer;

    public MessageConsumer(String topic, String id) {

        this.kafkaConsumer =  new KafkaConsumer<String,String>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                        ConsumerConfig.GROUP_ID_CONFIG, id,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"
                )
        );
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        kafkaConsumer.subscribe(Collections.singletonList("metadata"));
        kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> System.out.println(m.value()));
    }
}