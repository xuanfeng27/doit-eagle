package cn._51doit.jobs;

import cn._51doit.pojo.OrderMain;
import cn._51doit.udfs.JsonToOrderMainBeanFunction;
import cn._51doit.utils.FlinkUtils;
import cn._51doit.utils.FlinkUtilsV2;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 订单多维度统计
 */
public class OrderCount {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        String orderMainTopic = parameterTool.getRequired("kafka.input.main");

        DataStream<String> orderMainStream = FlinkUtilsV2.createKafkaStream(parameterTool, orderMainTopic, SimpleStringSchema.class);

        String orderDetailTopic = parameterTool.getRequired("kafka.input.detail");

        DataStream<String> orderDetailStream = FlinkUtilsV2.createKafkaStream(parameterTool, orderDetailTopic, SimpleStringSchema.class);


        //使用ProcessFunction解析并过滤数据
        SingleOutputStreamOperator<OrderMain> orderMainBeanStream = orderMainStream.process(new JsonToOrderMainBeanFunction());


        FlinkUtilsV2.env.execute();



    }
}
