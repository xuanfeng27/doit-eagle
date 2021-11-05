package cn._51doit.udfs;

import cn._51doit.pojo.ItemEventCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * 对数据进行排序，求TopN
 *
 * 按照EventTime划分的滑动窗口，窗口按照EventTime的先后数据触发的
 * 窗口先触发的数据，先进入到ProcessFunction中，同一个窗口内，相同的key的数据会有多条，什么时候说明该窗口的数据已经全部到齐了呢？
 * 因为要排序，就必须将属于同一个窗口的数据集齐，然后才能排序
 *
 * 最重要的一个问题：什么时候数据才算攒齐了，才可以排序（下一个窗口的数据来了）
 *
 * 将同一个窗口内的数据先攒起来（存到状态中）
 *
 */
public class HotGoodTopNFunction extends KeyedProcessFunction<Tuple4<Long, Long, String, String>, ItemEventCount, ItemEventCount> {

    private ListState<ItemEventCount> listState;

    //攒起来的数据要保存到状态中
    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<ItemEventCount> stateDescriptor = new ListStateDescriptor<>("item-state", ItemEventCount.class);
        listState = getRuntimeContext().getListState(stateDescriptor);
    }

    @Override
    public void processElement(ItemEventCount value, Context ctx, Collector<ItemEventCount> out) throws Exception {

        //将同一个窗口的数据先攒起来
        listState.add(value);
        //注册一个定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    //当下一个窗口的数据到达，对应的watermark一定会大于value.windowEnd + 1，onTimer方法就会被调用
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemEventCount> out) throws Exception {

        ArrayList<ItemEventCount> itemList = (ArrayList<ItemEventCount>) listState.get();
        //排序
        itemList.sort(new Comparator<ItemEventCount>() {
            @Override
            public int compare(ItemEventCount o1, ItemEventCount o2) {
                return Long.compare(o2.count, o1.count);
            }
        });
        //输出Top3
        for (int i = 0; i < Math.min(3, itemList.size()); i++) {
            ItemEventCount itemEventCount = itemList.get(i);
            itemEventCount.sort = (i + 1);
            out.collect(itemEventCount);
        }
        //清空当前窗口的数据
        listState.clear();
    }
}
