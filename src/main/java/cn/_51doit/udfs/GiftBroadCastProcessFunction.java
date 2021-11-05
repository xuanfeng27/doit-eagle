package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;


public class GiftBroadCastProcessFunction extends BroadcastProcessFunction<DataBean, Tuple3<String, String, Double>, Tuple3<String, String, Double>> {

    private MapStateDescriptor<String, Tuple2<String, Double>> broadcastStateDesc;

    public GiftBroadCastProcessFunction(MapStateDescriptor<String, Tuple2<String, Double>> broadcastStateDesc) {
        this.broadcastStateDesc = broadcastStateDesc;
    }

    @Override
    public void processElement(DataBean bean, ReadOnlyContext ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {

        ReadOnlyBroadcastState<String, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(broadcastStateDesc);

        String anchorId = bean.getProperties().get("anchor_id").toString();
        String giftId = bean.getProperties().get("gift_id").toString();
        Tuple2<String, Double> tp = broadcastState.get(giftId);
        out.collect(Tuple3.of(anchorId, tp.f0, tp.f1));
    }

    //处理广播数据的
    @Override
    public void processBroadcastElement(Tuple3<String, String, Double> giftTp, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {

        String id = giftTp.f0;
        String name = giftTp.f1;
        Double score = giftTp.f2;
        BroadcastState<String, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
        broadcastState.put(id, Tuple2.of(name, score));

    }
}
