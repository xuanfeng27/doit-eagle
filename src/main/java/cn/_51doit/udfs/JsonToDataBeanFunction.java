package cn._51doit.udfs;

import cn._51doit.pojo.DataBean;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将JSON字符串转成Bean对象，如果有问题数据，并且将数据过滤
 *
 * 使用ProcessFunction（NoKeyed）
 */
public class JsonToDataBeanFunction extends ProcessFunction<String, DataBean> {

    @Override
    public void processElement(String value, Context ctx, Collector<DataBean> out) throws Exception {

        try {
            DataBean dataBean = JSON.parseObject(value, DataBean.class);
            out.collect(dataBean);
        } catch (Exception e) {
            //e.printStackTrace();
            //TODO 将有问题的数据保存起来
        }

    }
}
