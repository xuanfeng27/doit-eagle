package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import cn._51doit.utils.Constants;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AudienceCountFunction extends KeyedProcessFunction<String, DataBean, Tuple3<String, Integer, Integer>> {

    private transient MapState<String, Integer> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<>("user-count-state", String.class, Integer.class);
        mapState = getRuntimeContext().getMapState(stateDescriptor);
    }

    @Override
    public void processElement(DataBean value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

        String eventId = value.getEventId();

        Integer onLineAudienceCount = mapState.get(Constants.ONLINE_AUDIENCE);
        Integer totalAudienceCount = mapState.get(Constants.TOTAL_AUDIENCE);

        if (onLineAudienceCount == null) {
            onLineAudienceCount = 0;
        }
        if (totalAudienceCount == null) {
            totalAudienceCount = 0;
        }
        //进入事件
        if (Constants.LIVE_ENTER.equals(eventId)) {
            //实时在线人数+1
            onLineAudienceCount += 1;
            //累计观众人数+1
            totalAudienceCount += 1;
        } else if (Constants.LIVE_LEAVE.equals(eventId)) {
            //实时在线人数-1
            onLineAudienceCount -= 1;
        }
        mapState.put(Constants.ONLINE_AUDIENCE, onLineAudienceCount);
        mapState.put(Constants.TOTAL_AUDIENCE, totalAudienceCount);
        //输出数据
        out.collect(Tuple3.of(ctx.getCurrentKey(), onLineAudienceCount, totalAudienceCount));

    }



}
