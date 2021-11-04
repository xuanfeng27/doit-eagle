package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.AudienceCountFunctionV3;
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

/**
 * 统计在直播间停留60秒或以上的用户为有效的累计用户
 *
 * 使用定时器（KeyedStream + Timer）
 *
 */
public class AudienceCountV3 {

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

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> res = keyedStream.process(new AudienceCountFunctionV3());

        res.print();

        FlinkUtils.env.execute();


    }

}
