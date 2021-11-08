package cn._51doit.udfs;

import cn._51doit.proto.DataBeanProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class JsonToProtoBufFunction extends ProcessFunction<Tuple2<String, String>, DataBeanProto.DataBean> {

    @Override
    public void processElement(Tuple2<String, String> tp, Context ctx, Collector<DataBeanProto.DataBean> out) throws Exception {

        try {
            String id = tp.f0;
            String json = tp.f1;
            DataBeanProto.DataBean.Builder bean = DataBeanProto.DataBean.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(json, bean);
            bean.setId(id);
            out.collect(bean.build());
        } catch (InvalidProtocolBufferException e) {
            //e.printStackTrace();
        }


    }
}
