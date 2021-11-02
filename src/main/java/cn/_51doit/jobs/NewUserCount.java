package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.JsonToDataBeanFunction;
import cn._51doit.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 使用Flink做实时多维度统计，那么就要跟单多个条件，进行很多次keyBy，然后再进行sum
 * 这样会导致整个job效率变低，所有使用Flink做多维度的聚合操作，不太适合
 * 所有，咱们使用Flink做一些预处理，如果做实时多维度统计，可以使用ClickHouse和DorisDB（StarRocks）
 *
 */

public class NewUserCount {

    public static void main(String[] args) throws Exception{

        DataStream<String> lines = FlinkUtils.createKafkaStream(args[0], SimpleStringSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = lines.process(new JsonToDataBeanFunction());

        //多维度统计新用户(手机型号、下载渠道)
        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> bean.getEventId().equals("appLaunch"));

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tpStream = filtered.map(new MapFunction<DataBean, Tuple3<String, String, Integer>>() {

            @Override
            public Tuple3<String, String, Integer> map(DataBean value) throws Exception {
                String deviceType = value.getDeviceType();
                String releaseChannel = value.getReleaseChannel();
                int isNew = value.getIsNew();
                return Tuple3.of(deviceType, releaseChannel, isNew);
            }
        });

//        beanStream.map(new MapFunction<DataBean, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> map(DataBean value) throws Exception {
//                return Tuple2.of(value.getIsNew(), 1);
//            }
//        }).keyBy(t -> t.f0).print();
        

        //计算设备类型
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resByDeviceType = tpStream.keyBy(t -> t.f0).sum(2);

        //计算下载渠道
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resByReleaseChannel = tpStream.keyBy(t -> t.f1).sum(2);

        //统计两个维度组合
        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> resByDeviceTypeAndReleaseChannel  = tpStream.keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        });


        //以后将将结果保存的Redis中
        resByDeviceType.print();

        resByReleaseChannel.print();

        resByDeviceTypeAndReleaseChannel.print();
        

        FlinkUtils.env.execute();


    }
}
