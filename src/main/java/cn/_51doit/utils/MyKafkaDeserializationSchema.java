package cn._51doit.utils;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 自定义Kafka的反序列化器，将topic-partition-offset拼接起来作为数据的唯一ID，同时将数据放入到一个元祖中
 */
public class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple2<String, String>> {

    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String line = new String(record.value());
        String id = topic + "-" + partition + "-" + offset;
        return Tuple2.of(id, line);
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
    }
}
