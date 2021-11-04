package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import cn._51doit.utils.Constants;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class AudienceWindowAggFunction implements AggregateFunction<DataBean, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    //（实时在线、累计观众）
    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(DataBean bean, Tuple2<Integer, Integer> accumulator) {
        String eventId = bean.getEventId();
        //进入时间
        if (Constants.LIVE_ENTER.equals(eventId)) {
            accumulator.f0 += 1;
            accumulator.f1 += 1;
        } else if (Constants.LIVE_LEAVE.equals(eventId)) {
            accumulator.f0 -= 1;
        }
        return accumulator;
    }

    @Override
    public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
        return null;
    }
}
