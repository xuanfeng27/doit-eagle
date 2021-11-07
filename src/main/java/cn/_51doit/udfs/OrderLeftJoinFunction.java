package cn._51doit.udfs;

import cn._51doit.pojo.OrderDetail;
import cn._51doit.pojo.OrderMain;
import org.apache.commons.collections.OrderedMap;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class OrderLeftJoinFunction implements CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>> {

    @Override
    public void coGroup(Iterable<OrderDetail> first, Iterable<OrderMain> second, Collector<Tuple2<OrderDetail, OrderMain>> out) throws Exception {

        for (OrderDetail orderDetail : first) {
            boolean isJoined = false;
            for (OrderMain orderMain : second) {
                isJoined = true;
                out.collect(Tuple2.of(orderDetail, orderMain));
            }
            if(!isJoined) {
                out.collect(Tuple2.of(orderDetail, null));
            }
        }

    }
}
