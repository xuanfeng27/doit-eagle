package cn._51doit.jobs;

import cn._51doit.pojo.OrderDetail;
import cn._51doit.pojo.OrderMain;
import cn._51doit.udfs.JsonToOrderDetailBeanFunction;
import cn._51doit.udfs.JsonToOrderMainBeanFunction;
import cn._51doit.udfs.OrderLeftJoinFunction;
import cn._51doit.utils.FlinkUtils;
import cn._51doit.utils.FlinkUtilsV2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 订单多维度统计
 */
public class OrderCount {

    public static void main(String[] args) throws Exception {

        //用于做实验，生产环境不能这样设置
        FlinkUtilsV2.env.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        String orderMainTopic = parameterTool.getRequired("kafka.input.main");

        DataStream<String> orderMainStream = FlinkUtilsV2.createKafkaStream(parameterTool, orderMainTopic, SimpleStringSchema.class);

        String orderDetailTopic = parameterTool.getRequired("kafka.input.detail");

        DataStream<String> orderDetailStream = FlinkUtilsV2.createKafkaStream(parameterTool, orderDetailTopic, SimpleStringSchema.class);


        //使用ProcessFunction解析并过滤数据
        SingleOutputStreamOperator<OrderMain> orderMainBeanStream = orderMainStream.process(new JsonToOrderMainBeanFunction());
        SingleOutputStreamOperator<OrderDetail> orderDetailBeanStream = orderDetailStream.process(new JsonToOrderDetailBeanFunction());

        //按照EventTime划分窗口
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMark = orderDetailBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getUpdate_time().getTime();
            }
        }));

        SingleOutputStreamOperator<OrderMain> orderMainWithWaterMark = orderMainBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderMain>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderMain>() {
            @Override
            public long extractTimestamp(OrderMain element, long recordTimestamp) {
                return element.getUpdate_time().getTime();
            }
        }));

        //将两个流进行join
        //明显表LeftOuterJoin主表
        DataStream<Tuple2<OrderDetail, OrderMain>> joinedStream = orderDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                .where(bean -> bean.getOrder_id()) //明显表关联的条件
                .equalTo(bean -> bean.getOid()) //主表的关联条件
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) //窗口长度为5秒的滚动窗口
                .apply(new OrderLeftJoinFunction());

        joinedStream.print();

        FlinkUtilsV2.env.execute();



    }
}
