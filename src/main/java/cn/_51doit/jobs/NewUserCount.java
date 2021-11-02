package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.JsonToDataBeanFunction;
import cn._51doit.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class NewUserCount {

    public static void main(String[] args) throws Exception{

        DataStream<String> lines = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = lines.process(new JsonToDataBeanFunction());

        beanStream.print();

        FlinkUtils.env.execute();


    }
}
