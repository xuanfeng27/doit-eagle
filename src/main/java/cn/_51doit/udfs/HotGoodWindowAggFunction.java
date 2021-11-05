package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 在窗口内进行增量聚合
 */
public class HotGoodWindowAggFunction implements AggregateFunction<DataBean, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(DataBean value, Long accumulator) {
        return accumulator += 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
