import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Application {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    public static final String TOPIC = "events";

    public static void main(String[] args) {
        KafkaProducer<Long, String> producer = createProducer(BOOTSTRAP_SERVERS);
        produceMessages(100, producer);
    }

    public static void produceMessages(int number, KafkaProducer<Long, String> producer) {
        for (int i=0; i<number; ++i) {
            String value = String.format("event_%d", i);
            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, (long) i, value);
            try {
                RecordMetadata recordMetadata = producer.send(record).get();
                System.out.printf("record key=[%d], value=[%s], partition=[%d], offset=[%d]\n",
                        i, value, recordMetadata.partition(), recordMetadata.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

    }

    public static KafkaProducer<Long, String> createProducer(String bootStrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<Long, String>(properties);
    }
}
