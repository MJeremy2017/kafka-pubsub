import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class Consumer {
    public static final String topic = "events";
    public static final String SERVERS = "localhost:9092,localhost:9093";

    public static void main(String[] args) {
        String consumerGroup = args.length == 1 ? args[0] : "defaultConsumerGroup";
        System.out.println("Using consumer group: " + consumerGroup);

        KafkaConsumer<Long, String> consumer = createConsumer(SERVERS, consumerGroup);
        consumeMessages(consumer, topic);
    }


    public static void consumeMessages(KafkaConsumer<Long, String> consumer, String topic) {
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(10));  // timeout duration
            for (ConsumerRecord<Long, String> record : records) {
                System.out.printf("Record key=%d, value=%s, partition=%d, offset=%d\n",
                        record.key(), record.value(), record.partition(), record.offset());
            }

            consumer.commitAsync();
        }
    }

    public static KafkaConsumer<Long, String> createConsumer(String bootstrapServer, String consumerGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<Long, String>(properties);
    }

}
