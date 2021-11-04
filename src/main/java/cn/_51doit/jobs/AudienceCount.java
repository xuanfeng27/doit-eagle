package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.AudienceCountFunction;
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
 * 根据主播，统计实时在线人数、累计观众数
 */
public class AudienceCount {

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

        //根据主播ID统计在线人数和累计观众
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> res = keyedStream.process(new AudienceCountFunction());

        //打印或存储到Redis、MySQL，每输入一条数据，输出一条结果
        //问题：对外部的存储系统压力太大（外部的的数据，压力过大，导致没法正常使用了）
        res.print();

        FlinkUtils.env.execute();

    }
}
