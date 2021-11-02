package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将JSON字符串转成Bean对象，如果有问题数据，并且将数据过滤
 *
 * 使用ProcessFunction（NoKeyed）
 */
public class JsonToDataBeanFunctionV2 extends ProcessFunction<Tuple2<String, String>, DataBean> {

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<DataBean> out) throws Exception {

        try {
            DataBean dataBean = JSON.parseObject(value.f1, DataBean.class);
            dataBean.setId(value.f0);
            out.collect(dataBean);
        } catch (Exception e) {
            //e.printStackTrace();
            //TODO 将有问题的数据保存起来
        }

    }

}
