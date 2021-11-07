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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

        OutputTag<OrderDetail> lateDataTag = new OutputTag<OrderDetail>("late-order-detail-tag") {
        };

        //存在的问题（最怕明显表的数据迟到）
        //解决方案1（最好,修改CoGroup源码）
        //解决方案2（效率稍微低一点）
        //使用的方案为方案2，实现将订单明细表的的数据划分一个窗口（窗口的长度和窗口的类型于后面join的一致）
        //如果订单明细的数据，进入到前面的窗口内迟到了，那也说明进入到后面的窗口肯定也迟到了
        SingleOutputStreamOperator<OrderDetail> windowStream = orderDetailWithWaterMark.keyBy(bean -> bean.getOrder_id())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lateDataTag)  //将迟到的数据打上标签
                .apply(new WindowFunction<OrderDetail, OrderDetail, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<OrderDetail> input, Collector<OrderDetail> out) throws Exception {
                        for (OrderDetail orderDetail : input) {
                            out.collect(orderDetail);
                        }
                    }
                });


        //获取OrderDetail迟到的数据
        DataStream<OrderDetail> lateOrderDetailStream = windowStream.getSideOutput(lateDataTag);
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> lateOrderTupleStream = lateOrderDetailStream.map(new MapFunction<OrderDetail, Tuple2<OrderDetail, OrderMain>>() {
            @Override
            public Tuple2<OrderDetail, OrderMain> map(OrderDetail value) throws Exception {
                return Tuple2.of(value, null);
            }
        });


        //将两个流进行join
        //明显表LeftOuterJoin主表
        DataStream<Tuple2<OrderDetail, OrderMain>> joinedStream = orderDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                .where(bean -> bean.getOrder_id()) //明显表关联的条件
                .equalTo(bean -> bean.getOid()) //主表的关联条件
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) //窗口长度为5秒的滚动窗口
                .apply(new OrderLeftJoinFunction());


        //将cogroup的流和迟到的流，union到一起，使用相同的方式进行处理
        DataStream<Tuple2<OrderDetail, OrderMain>> unionStream = joinedStream.union(lateOrderTupleStream);

        //将没有关联上主表的数据，查询数据库关联主表数据
        SingleOutputStreamOperator<Tuple2<OrderDetail, OrderMain>> res = unionStream.map(new MapFunction<Tuple2<OrderDetail, OrderMain>, Tuple2<OrderDetail, OrderMain>>() {

            @Override
            public Tuple2<OrderDetail, OrderMain> map(Tuple2<OrderDetail, OrderMain> tp) throws Exception {

                //没有关联上主表的数据
                if (tp.f1 == null) {
                    //查询数据库关联
                    OrderMain orderMain = QueryOrderMainByOrderId(tp.f0.getOrder_id());
                    tp.f1 = orderMain;
                }
                return tp;
            }
        });

        //将数据写入到ClickHouse中（做多维度的实时查询）


        joinedStream.print();

        FlinkUtilsV2.env.execute();



    }


    private static OrderMain QueryOrderMainByOrderId(Long oid) {

        //创建数据库连接
        //产线数据库
        //返回结果
        return null;
    }
}

