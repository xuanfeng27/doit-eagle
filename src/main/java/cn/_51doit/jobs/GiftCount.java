package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.GiftBroadCastProcessFunction;
import cn._51doit.udfs.JsonToDataBeanFunction;
import cn._51doit.utils.Constants;
import cn._51doit.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;

/**
 * 礼物相关数据统计
 * 根据主播ID，统计礼物的数量、礼物的积分
 *
 * 礼物的数据(id,礼物名称,积分)是存在在MySQL中的，而且数据量比较小，并且可能会变化
 * 使用广播状态，将礼物相关的数据广播出去
 */
public class GiftCount {

    public static void main(String[] args) throws Exception {

        DataStreamSource<String> lines = FlinkUtils.env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> giftStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        //定义广播状态的描述器，指定广播后的状态，在下游以何种方式存储
        MapStateDescriptor<String, Tuple2<String, Double>> broadcastStateDesc =  new MapStateDescriptor<String, Tuple2<String, Double>>("gift-broadcast-state", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}));

        BroadcastStream<Tuple3<String, String, Double>> broadcastStream = giftStream.broadcast(broadcastStateDesc);

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = kafkaStream.process(new JsonToDataBeanFunction());

        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> Constants.LIVE_REWARD.equals(bean.getEventId()));

        //先keyBy，在关联数据
        //KeyedStream<DataBean, String> keyedStream = filtered.keyBy(bean -> bean.getProperties().get("anchor_id").toString());

        //事实流connect广播流(共享状态)
        SingleOutputStreamOperator<Tuple4<String, String, Integer, Double>> connectedStream = filtered.connect(broadcastStream).process(new GiftBroadCastProcessFunction(broadcastStateDesc));

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Double>> res = connectedStream.keyBy(tp -> tp.f0).reduce(new ReduceFunction<Tuple4<String, String, Integer, Double>>() {
            @Override
            public Tuple4<String, String, Integer, Double> reduce(Tuple4<String, String, Integer, Double> value1, Tuple4<String, String, Integer, Double> value2) throws Exception {
                //累计礼物数量
                value1.f2 += value2.f2;
                //累计礼物积分
                value1.f3 += value2.f3;
                return value1;
            }
        });

        res.print();

        FlinkUtils.env.execute();

    }
}
