package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.IsNewUserFunction;
import cn._51doit.udfs.IsNewUserFunctionV2;
import cn._51doit.udfs.JsonToDataBeanFunction;
import cn._51doit.udfs.JsonToDataBeanFunctionV2;
import cn._51doit.utils.FlinkUtils;
import cn._51doit.utils.MyKafkaDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;


/**
 * 判断用户是否是新用户的终极解决方案
 * 1.按照设备ID进行KeyBy，设备ID相同的一大会近日到同一个分区中，但是同一个分区有可能有多个不同的设备ID
 * 2.一起使用设备型号进行Key，然后使用布隆过滤器保存用户ID，存在两个问题（①可能会数据倾斜②布隆过滤器不准确）
 * 3.如果使用按照设备ID进行KeyBy，是将设备ID保存到KeyedState中的key，但是会导致KeyedState中的key非常多，使用是RocksDB StateBackend
 * 4.RocksDB StateBackend可以存更多的状态（大状态），而且可以进行增量checkpoint
 */
public class IsNewUserV2 {

    public static void main(String[] args) throws Exception{


        DataStream<Tuple2<String, String>> steamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataBean> beanStream = steamWithId.process(new JsonToDataBeanFunctionV2());
        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> "appLaunch".equals(bean.getEventId()));

        //按照设备ID进行keyBY
        KeyedStream<DataBean, String> keyedStream = filtered.keyBy(DataBean::getDeviceId);

        //还有配置RocksDB StateBackend
        SingleOutputStreamOperator<DataBean> res = keyedStream.process(new IsNewUserFunctionV2());

        res.print();

        FlinkUtils.env.execute();




    }
}
