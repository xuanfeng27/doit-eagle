package cn._51doit.jobs;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 拼团相关主表统计
 * <p>
 * 模拟的简化数据
 * //拼团主表的数据
 * //时间,拼团主表ID、拼团状态（1拼团中，2拼团完成，3已取消）、商品分类或分类ID
 * 1000,p201,1,手机
 * 5001,p202,1,手机
 * <p>
 * //拼团明细表的数据
 * //时间，用户ID，拼团主表ID，订单主表ID
 * 1001,u1646,p201,o1002
 * 5001,u1647,p202,o1003
 * <p>
 * //订单主表
 * //时间，订单ID，订单状况，订单金额
 * 1002,o1002,101,2000.0
 * 5002,o1003,101,3000.0
 * <p>
 * 由于想要的数据来自多个流，必须将多个流进行join，Flink无法直接将一个cogroup多个流，但是可以先两个流进行cgroup，在跟下一个流进行cogroup
 * <p>
 * <p>
 * 拼团明细表 LeftJoin 订单主表 LeftJoin 拼团主表
 */
public class GrouponCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //拼团明细表(时间,用户id,拼团主表id,订单主表ID)
        //1001,u1646,p201,o1002
        //5001,u1647,p202,o1003
        DataStreamSource<String> grouponDetailStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> grouponDetailStreamWithWaterMark = grouponDetailStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String line, long l) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpGrouponDetailStream = grouponDetailStreamWithWaterMark.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[1], fields[2], fields[3]);
            }
        });


        //拼团主表(时间,拼团主表ID，拼团状态，商品分类)
        //1000,p201,1,手机
        //5001,p202,1,手机
        DataStreamSource<String> grouponMainStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> grouponMainStreamWithWaterMark = grouponMainStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String line, long l) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpGrouponMainStream = grouponMainStreamWithWaterMark.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[1], fields[2], fields[3]);
            }
        });

        //订单主表（时间、订单id，订单状态、订单金额）
        //1002,o1002,101,2000.0
        //5002,o1003,101,3000.0
        DataStreamSource<String> orderMainStream = env.socketTextStream("localhost", 11111);

        SingleOutputStreamOperator<String> orderMainStreamWithWaterMark = orderMainStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String line, long l) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpOrderMainStream = orderMainStreamWithWaterMark.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[1], fields[2], Double.parseDouble(fields[3]));
            }
        });

        //先用拼团明显表 leftJoin 拼团主表
        //将拼团明细表跟拼团主表进行cogroup
        DataStream<Tuple5<String, String, String, String, String>> joinedStream1 = tpGrouponDetailStream.coGroup(tpGrouponMainStream)
                .where(f -> f.f1)
                .equalTo(s -> s.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, String>> first, Iterable<Tuple3<String, String, String>> second, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {

                        for (Tuple3<String, String, String> tp1 : first) {
                            boolean isJoined = false;
                            for (Tuple3<String, String, String> tp2 : second) {
                                out.collect(Tuple5.of(tp1.f0, tp1.f1, tp1.f2, tp2.f1, tp2.f2));
                                isJoined = true;
                            }
                            if (!isJoined) {
                                out.collect(Tuple5.of(tp1.f0, tp1.f1, tp1.f2, null, null));
                            }
                        }
                    }
                });

        //将两个流join的结果，再与订单主表进行join
        //拼团的两个流join后再跟订单流进行join
        DataStream<Tuple7<String, String, String, String, String, String, Double>> joinedStream2 = joinedStream1.coGroup(tpOrderMainStream)
                .where(f -> f.f2)
                .equalTo(s -> s.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple5<String, String, String, String, String>, Tuple3<String, String, Double>, Tuple7<String, String, String, String, String, String, Double>>() {
                    @Override
                    public void coGroup(Iterable<Tuple5<String, String, String, String, String>> first, Iterable<Tuple3<String, String, Double>> second, Collector<Tuple7<String, String, String, String, String, String, Double>> collector) throws Exception {
                        for (Tuple5<String, String, String, String, String> tp1 : first) {
                            boolean isJoined = false;
                            for (Tuple3<String, String, Double> tp2 : second) {
                                collector.collect(Tuple7.of(tp1.f0, tp1.f1, tp1.f2, tp1.f3, tp1.f4, tp2.f1, tp2.f2));
                                isJoined = true;
                            }
                            if (!isJoined) {
                                collector.collect(Tuple7.of(tp1.f0, tp1.f1, tp1.f2, tp1.f3, tp1.f4, null, null));
                            }
                        }
                    }
                });

        joinedStream2.print();

        env.execute();


    }


}
