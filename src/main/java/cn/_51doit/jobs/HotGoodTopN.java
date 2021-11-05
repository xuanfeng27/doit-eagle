package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.pojo.ItemEventCount;
import cn._51doit.udfs.HotGoodTopNFunction;
import cn._51doit.udfs.HotGoodWindowAggFunction;
import cn._51doit.udfs.HotGoodWindowProcessFunction;
import cn._51doit.udfs.JsonToDataBeanFunction;
import cn._51doit.utils.FlinkUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 1.在窗口内增量聚合：按照分类ID、事件ID
 * 2.将增量聚合后的结果在求topN：
 */
public class HotGoodTopN {

    public static void main(String[] args) throws Exception {

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new JsonToDataBeanFunction());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> bean.getEventId().startsWith("product"));

        SingleOutputStreamOperator<DataBean> beanStreamWithWaterMark = filtered.assignTimestampsAndWatermarks(WatermarkStrategy.<DataBean>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<DataBean>() {
            @Override
            public long extractTimestamp(DataBean bean, long recordTimestamp) {
                return bean.getTimestamp();
            }
        }));

        //按照分类ID、事件ID、商品ID
        KeyedStream<DataBean, Tuple3<String, String, String>> keyedStream = beanStreamWithWaterMark.keyBy(new KeySelector<DataBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(DataBean value) throws Exception {
                String eventId = value.getEventId();
                String categoryId = value.getProperties().get("category_id").toString();
                String productId = value.getProperties().get("product_id").toString();
                return Tuple3.of(categoryId, eventId, productId);
            }
        });

        //按照EventTime划分滑动窗口
        WindowedStream<DataBean, Tuple3<String, String, String>, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)));
        //1.将数据在窗口内增量聚合；2.获取数据的所在窗口，即窗口的起始时间和结束时间
        SingleOutputStreamOperator<ItemEventCount> aggStream = windowedStream.aggregate(new HotGoodWindowAggFunction(), new HotGoodWindowProcessFunction());
        //按照、同一个窗口、商品分类、事件类型进行keyBy
        KeyedStream<ItemEventCount, Tuple4<Long, Long, String, String>> keyedStream2 = aggStream.keyBy(new KeySelector<ItemEventCount, Tuple4<Long, Long, String, String>>() {
            @Override
            public Tuple4<Long, Long, String, String> getKey(ItemEventCount item) throws Exception {
                long windowStart = item.windowStart;
                long windowEnd = item.windowEnd;
                String categoryId = item.categoryId;
                String eventId = item.eventId;
                return Tuple4.of(windowStart, windowEnd, categoryId, eventId);
            }
        });
        //在ProcessFunction对数据进行【排序】
        SingleOutputStreamOperator<ItemEventCount> res = keyedStream2.process(new HotGoodTopNFunction());

        res.print();


    }

}
