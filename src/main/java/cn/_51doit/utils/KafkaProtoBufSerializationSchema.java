package cn._51doit.utils;

import cn._51doit.proto.DataBeanProto;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaProtoBufSerializationSchema implements KafkaSerializationSchema<DataBeanProto.DataBean> {

    private String topic;

    public KafkaProtoBufSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(DataBeanProto.DataBean bean, @Nullable Long timestamp) {
        //将ProtoBuf的bean转成了protoBuf的二进制，保存到Kafka中
        return new ProducerRecord<>(topic, bean.toByteArray());
    }
}
