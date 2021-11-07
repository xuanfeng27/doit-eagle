package cn._51doit.udfs;

import cn._51doit.pojo.OrderDetail;
import cn._51doit.pojo.OrderMain;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class JsonToOrderDetailBeanFunction extends ProcessFunction<String, OrderDetail> {


    @Override
    public void processElement(String value, Context ctx, Collector<OrderDetail> out) throws Exception {
        //解析JSON
        //过滤数据
        try {
            JSONObject jsonObject = JSON.parseObject(value);
            String type = jsonObject.getString("type");
            if ("INSERT".equals(type) || "UPDATE".equals(type) || "DELETE".equals(type)) {
                JSONArray jsonArray = jsonObject.getJSONArray("data");
                //将数据打平
                for (int i = 0; i < jsonArray.size(); i++) {
                    OrderDetail orderDetail = jsonArray.getObject(i, OrderDetail.class);
                    orderDetail.setType(type); //设置数据操作类型
                    out.collect(orderDetail);
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
            //有问的数据记录下来
        }

    }
}
