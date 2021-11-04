package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import cn._51doit.utils.Constants;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

public class AudienceCountFunctionV3 extends KeyedProcessFunction<String, DataBean, Tuple3<String, Integer, Integer>> {

    //MapState<String, Long> //sessionId，进入时间
    private transient MapState<String, Long> mapState;

    //ValueState<Integer> //有效累计观众数量
    private transient ValueState<Integer> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("uid-time-state", String.class, Long.class);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);

        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("total-user-state", Integer.class);
        valueState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(DataBean bean, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

        //只算有效累计用户
        String eventId = bean.getEventId();
        String sessionId = bean.getProperties().get("live_session").toString();
        //进入事件
        if (Constants.LIVE_ENTER.equals(eventId)) {
            long currentTimeMillis = System.currentTimeMillis();
            mapState.put(sessionId, currentTimeMillis);
            //注册定时器
            ctx.timerService().registerProcessingTimeTimer(currentTimeMillis + 60000 + 1);
        } else if (Constants.LIVE_LEAVE.equals(eventId)) {
            //离开事件
            Long enterTime = mapState.get(sessionId);
            if (enterTime != null) {
                mapState.remove(sessionId);
            }
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

        Integer totalCount = valueState.value();
        if (totalCount == null) totalCount = 0;

        Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
        while (iterator.hasNext()) {

            Map.Entry<String, Long> entry = iterator.next();
            Long enterTime = entry.getValue();
            if (timestamp - enterTime > 60000) {
                totalCount += 1;
                valueState.update(totalCount);
                //异常该回话的session，避免同一个回话反复被计算
                iterator.remove();
            }
        }

        out.collect(Tuple3.of(ctx.getCurrentKey(), 0, totalCount));

    }
}
