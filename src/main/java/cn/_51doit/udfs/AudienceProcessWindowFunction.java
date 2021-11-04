package cn._51doit.udfs;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AudienceProcessWindowFunction extends ProcessWindowFunction<Tuple2<Integer, Integer>, Tuple3<String, Integer, Integer>, String, TimeWindow> {

    private transient ValueState<Integer> onLineUserCountState;

    private transient ValueState<Integer> totalUserCountState;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Integer> onLineUserStateDesc = new ValueStateDescriptor<>("online-user-state", Integer.class);
        onLineUserCountState = getRuntimeContext().getState(onLineUserStateDesc);

        ValueStateDescriptor<Integer> totalUserStateDesc = new ValueStateDescriptor<>("total-user-state", Integer.class);
        totalUserCountState = getRuntimeContext().getState(totalUserStateDesc);

    }

    @Override
    public void process(String key, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

        Tuple2<Integer, Integer> tp = elements.iterator().next();
        Integer onLineUserCount = onLineUserCountState.value();
        Integer totalUserCount = totalUserCountState.value();
        if (onLineUserCount == null) {
            onLineUserCount = 0;
        }
        if (totalUserCount == null) {
            totalUserCount = 0;
        }
        onLineUserCount += tp.f0;
        onLineUserCountState.update(onLineUserCount);

        totalUserCount += tp.f1;
        totalUserCountState.update(totalUserCount);

        out.collect(Tuple3.of(key, onLineUserCount, totalUserCount));


    }
}
