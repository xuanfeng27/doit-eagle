package cn._51doit.day01;

import cn._51doit.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NewUserCount {

    public static void main(String[] args) throws Exception{

        DataStream<String> lines = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);

        lines.print();

        FlinkUtils.env.execute();


    }
}
