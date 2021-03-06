package cn._51doit.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtils {

    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(String path, Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        //Flink的工具类，用于参数解析
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        String topics = parameterTool.get("kafka.input.topics");
        long ckInterval = parameterTool.getLong("checkpoint.interval", 60000);
        String ckPath = parameterTool.getRequired("checkpoint.path");
        env.enableCheckpointing(ckInterval);
        env.setStateBackend(new FsStateBackend(ckPath));
        Properties properties = parameterTool.getProperties();

        List<String> topicList = Arrays.asList(topics.split(","));
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                properties
        );
        //不将偏移量写入到kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);


    }


    /**
     * 从Kafka中读取数据，并且生成唯一ID（topic-partition-offset）
     * @param path
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> DataStream<T> createKafkaStreamWithId(String path, Class<? extends KafkaDeserializationSchema<T>> clazz) throws Exception {
        //Flink的工具类，用于参数解析
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        String topics = parameterTool.get("kafka.input.topics");
        long ckInterval = parameterTool.getLong("checkpoint.interval", 60000);
        String ckPath = parameterTool.getRequired("checkpoint.path");
        env.enableCheckpointing(ckInterval);
        //env.setStateBackend(new FsStateBackend(ckPath));
        //使用RocksDB作为StateBackend，
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(ckPath);

        //Properties properties = parameterTool.getProperties();
        Properties properties = new Properties();
        String bootstrapServers = parameterTool.get("bootstrap.servers");
        String autoOffsetReset = parameterTool.get("auto.offset.reset", "earliest");
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("auto.offset.reset", autoOffsetReset);

        List<String> topicList = Arrays.asList(topics.split(","));
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                properties
        );

        //不将偏移量写入到kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);


    }
}
