package cn._51doit.udfs;

import cn._51doit.pojo.ItemEventCount;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口触发后，获取窗口中增量聚合的结果，并且回去数据所在窗口的起始时间，结束时间
 */
public class HotGoodWindowProcessFunction extends ProcessWindowFunction<Long, ItemEventCount, Tuple3<String, String, String>, TimeWindow> {

    //窗口触发后，每一个key会调用一次process方法
    @Override
    public void process(Tuple3<String, String, String> key, Context context, Iterable<Long> elements, Collector<ItemEventCount> out) throws Exception {
        //分类ID
        String categoryId = key.f0;
        //事件ID
        String eventId = key.f1;
        //商品ID
        String productId = key.f2;
        //获取窗口增量聚合的结果(次数)
        Long count = elements.iterator().next();
        //窗口的起始时间
        long windowStart = context.window().getStart();
        //窗口的结束时间
        long windowEnd = context.window().getEnd();
        //输出数据
        out.collect(new ItemEventCount(categoryId, productId, eventId, count, windowStart, windowEnd));
    }
}
