package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.IsNewUserFunction;
import cn._51doit.udfs.JsonToDataBeanFunction;
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
 * 根据用户的设备ID，判断用户是否是新用户
 * 【App（设备ID，即UDID）、小程序（openid）】、浏览器（cookie id）
 * 我们的项目注意分析的是手机设备
 *
 * 数据中，没有isNew这个字段，修改根据用户的历史数据，计算出是不是一个新用户
 *
 * 将用户的ID缓存起来（hashSet中），如果用了量比较大，缓存在HashSet内存中，会消耗大量资源
 *
 * 现在希望消耗的资源相对要少一些，可以使用布隆过滤器（缺点：有误判），由于判断新老用户，可以允许一定的误差，并且还有节省资源
 *
 * 注意：该程序不做多维度的统计，而是仅用户计算是否是新用户，最后将结果写入到ClickHoust中（多维统计）
 *
 *
 */
public class IsNewUser {

    public static void main(String[] args) throws Exception{

        DataStream<Tuple2<String, String>> steamWithId = FlinkUtils.createKafkaStreamWithId(args[0], MyKafkaDeserializationSchema.class);

        //SingleOutputStreamOperator<DataBean> beanStream = lines.process(new JsonToDataBeanFunction());

        //SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> "appLaunch".equals(bean.getEventId()));

        //KeyedStream<DataBean, String> keyedStream = filtered.keyBy(DataBean::getDeviceType);

        //如何让同一个设备ID的数据，一定进入的同一个分区中，然后将设备ID进行hash，存储到布隆过滤器中
        //存在的问题：可能会数据倾斜，并且一个分区中有多个布隆过滤器，会消耗更多的资源
        //SingleOutputStreamOperator<DataBean> res = keyedStream.process(new IsNewUserFunction());

        //res.print();
        //将数据写入到ClickHouse中


        steamWithId.print();





        FlinkUtils.env.execute();


    }
}
