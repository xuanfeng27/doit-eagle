package cn._51doit.utils;

import cn._51doit.proto.DataBeanProto;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class ProtoBufDeserializationSchema implements DeserializationSchema<DataBeanProto.DataBean> {

    @Override
    public DataBeanProto.DataBean deserialize(byte[] message) throws IOException {
        //将二进制转成ProtoBuf的Bean类型
        return DataBeanProto.DataBean.parseFrom(message);
    }

    @Override
    public boolean isEndOfStream(DataBeanProto.DataBean nextElement) {
        return false;
    }

    @Override
    public TypeInformation<DataBeanProto.DataBean> getProducedType() {
        return TypeInformation.of(DataBeanProto.DataBean.class);
    }
}
