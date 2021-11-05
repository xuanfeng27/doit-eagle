package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * 关联维度，不能计算指标
 */

public class GiftBroadCastProcessFunction extends BroadcastProcessFunction<DataBean, Tuple3<String, String, Double>, Tuple4<String, String, Integer, Double>> implements CheckpointedFunction {

    private MapStateDescriptor<String, Tuple2<String, Double>> broadcastStateDesc;

    private transient ListState<DataBean> listState;

    private int count = 0;

    public GiftBroadCastProcessFunction(MapStateDescriptor<String, Tuple2<String, Double>> broadcastStateDesc) {
        this.broadcastStateDesc = broadcastStateDesc;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<DataBean> stateDescriptor = new ListStateDescriptor<>("buffer-state", DataBean.class);
        listState = context.getOperatorStateStore().getListState(stateDescriptor);
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //
    }


    @Override
    public void processElement(DataBean bean, ReadOnlyContext ctx, Collector<Tuple4<String, String, Integer, Double>> out) throws Exception {
        //广播的状态还没有收集齐
        if (count < 9) {
            listState.add(bean);
        } else {
            ReadOnlyBroadcastState<String, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(broadcastStateDesc);

            ArrayList<DataBean> beanList = (ArrayList<DataBean>) listState.get();
            if (beanList != null && beanList.size() > 0) {
                for (DataBean dataBean : beanList) {
                    String anchorId = dataBean.getProperties().get("anchor_id").toString();
                    String giftId = dataBean.getProperties().get("gift_id").toString();
                    Tuple2<String, Double> tp = broadcastState.get(giftId);
                    String name = null;
                    Double score = null;
                    if (tp != null) {
                        name = tp.f0;
                        score = tp.f1;
                    }
                    out.collect(Tuple4.of(anchorId, name, 1,score));
                }
                listState.clear();
            }

            String anchorId = bean.getProperties().get("anchor_id").toString();
            String giftId = bean.getProperties().get("gift_id").toString();
            Tuple2<String, Double> tp = broadcastState.get(giftId);

            String name = null;
            Double score = null;
            if (tp != null) {
                name = tp.f0;
                score = tp.f1;
            }
            out.collect(Tuple4.of(anchorId, name, 1, score));
        }

    }

    //处理广播数据的
    @Override
    public void processBroadcastElement(Tuple3<String, String, Double> giftTp, Context ctx, Collector<Tuple4<String, String, Integer, Double>> out) throws Exception {

        String id = giftTp.f0;
        String name = giftTp.f1;
        Double score = giftTp.f2;
        BroadcastState<String, Tuple2<String, Double>> broadcastState = ctx.getBroadcastState(broadcastStateDesc);
        broadcastState.put(id, Tuple2.of(name, score));
        count += 1;
        System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " : " + count);
    }
}
