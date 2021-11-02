package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 根据设备型号进行KeyBy, 那一个设备型号对应一个布隆过滤器
 */
public class IsNewUserFunction extends KeyedProcessFunction<String, DataBean, DataBean> {

    private transient ValueState<BloomFilter<String>> bloomFilterState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<BloomFilter<String>> valueStateDescriptor = new ValueStateDescriptor<>("uid-bloom-filter-state", TypeInformation.of(new TypeHint<BloomFilter<String>>() {
        }));
        bloomFilterState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(DataBean value, Context ctx, Collector<DataBean> out) throws Exception {

        String deviceId = value.getDeviceId();

        BloomFilter<String> bloomFilter = bloomFilterState.value();

        if (bloomFilter == null) {
            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
        }
        //判断不在bloomFilter，就是新用户
        if (!bloomFilter.mightContain(deviceId)) {
            value.setIsN(1);
            bloomFilter.put(deviceId);
        }

        //更新状态
        bloomFilterState.update(bloomFilter);

        //输出数据
        out.collect(value);

    }
}
