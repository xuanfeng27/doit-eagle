package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.AudienceCountFunction;
import cn._51doit.udfs.AudienceProcessWindowFunction;
import cn._51doit.udfs.AudienceWindowAggFunction;
import cn._51doit.udfs.JsonToDataBeanFunctionV2;
import cn._51doit.utils.Constants;
import cn._51doit.utils.FlinkUtils;
import cn._51doit.utils.MyKafkaDeserializationSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 根据主播，统计实时在线人数、累计观众数
 *
 * 不再是来一条聚合一条了，而是在窗口内增量聚合，并且还要与历史数据进行聚合
 */
public class AudienceCountV2 {

    public static void main(String[] args) throws Exception {

        DataStream<Tuple2<String, String>> tpStream = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = tpStream.process(new JsonToDataBeanFunctionV2());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(new FilterFunction<DataBean>() {
            @Override
            public boolean filter(DataBean value) throws Exception {
                String eventId = value.getEventId();
                return Constants.LIVE_ENTER.equals(eventId) || Constants.LIVE_LEAVE.equals(eventId);
            }
        });

        KeyedStream<DataBean, String> keyedStream = filtered.keyBy(bean -> bean.getProperties().get("anchor_id").toString());

        //先keyby再划分窗口
        WindowedStream<DataBean, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(2)));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> res = windowedStream.aggregate(new AudienceWindowAggFunction(), new AudienceProcessWindowFunction());

        res.print();

        FlinkUtils.env.execute();


    }
}
