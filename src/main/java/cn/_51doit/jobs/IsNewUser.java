package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.udfs.IsNewUserFunction;
import cn._51doit.udfs.JsonToDataBeanFunction;
import cn._51doit.udfs.JsonToDataBeanFunctionV2;
import cn._51doit.utils.FlinkUtils;
import cn._51doit.utils.MyKafkaDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.SimpleDateFormat;
import java.util.Date;

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

        SingleOutputStreamOperator<DataBean> beanStream = steamWithId.process(new JsonToDataBeanFunctionV2());
        SingleOutputStreamOperator<DataBean> filtered = beanStream.filter(bean -> "appLaunch".equals(bean.getEventId()));

        KeyedStream<DataBean, String> keyedStream = filtered.keyBy(DataBean::getDeviceType);

        //如何让同一个设备ID的数据，一定进入的同一个分区中，然后将设备ID进行hash，存储到布隆过滤器中
        //存在的问题：可能会数据倾斜，并且一个分区中有多个布隆过滤器，会消耗更多的资源
        SingleOutputStreamOperator<DataBean> res = keyedStream.process(new IsNewUserFunction());

        //对数据进行处理
        res.map(new MapFunction<DataBean, DataBean>() {
            private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH");
            @Override
            public DataBean map(DataBean bean) throws Exception {
                //将数据生成的时间取出来，转成yyyyMMdd-HH
                Long timestamp = bean.getTimestamp();
                //20211101-13
                String format = dateFormat.format(new Date(timestamp));
                String[] fields = format.split("-");
                //设置日期20211101
                bean.setDate(fields[0]);
                //设置小时13
                bean.setHour(fields[1]);
                return bean;
            }
            //将数据写入到ClickHouse中
        }).addSink(JdbcSink.sink(
                "insert into tb_user_event(id, deviceId, eventId, isNew, os, province, channel, deviceType, eventTime, date, hour) values (?,?,?,?,?,?,?,?,?,?,?)",
                (ps, bean) -> {
                    ps.setString(1, bean.getId());
                    ps.setString(2, bean.getDeviceId());
                    ps.setString(3, bean.getEventId());
                    ps.setInt(4, bean.getIsNew());
                    ps.setString(5, bean.getOsName());
                    ps.setString(6, bean.getProvince());
                    ps.setString(7, bean.getReleaseChannel());
                    ps.setString(8, bean.getDeviceType());
                    ps.setLong(9, bean.getTimestamp());
                    ps.setString(10, bean.getDate());
                    ps.setString(11, bean.getHour());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(2000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://node-1.51doit.cn:8123/doitedu?characterEncoding=utf-8")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()));









        FlinkUtils.env.execute();


    }
}
