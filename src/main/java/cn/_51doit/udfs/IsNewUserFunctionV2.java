package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class IsNewUserFunctionV2 extends KeyedProcessFunction<String, DataBean, DataBean> {

    private transient ValueState<Byte> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Byte> stateDescriptor = new ValueStateDescriptor<>("uid-val-state", Byte.class);
        valueState = getRuntimeContext().getState(stateDescriptor);
        //以后可以将Redis中的设备ID读取过滤，保存到KeyedState中

    }

    @Override
    public void processElement(DataBean bean, Context ctx, Collector<DataBean> out) throws Exception {

        Byte bt = valueState.value();
        //如果bt为null，说明当前输入的这个设备ID，在该分区不存在，就是一个新用户
        if (bt == null) {
            //当前设备ID为新用户
            bean.setIsN(1);
            valueState.update((byte) 1);
        }
        out.collect(bean);
    }


}
