package cn._51doit.jobs;

import cn._51doit.pojo.DataBean;
import cn._51doit.proto.DataBeanProto;
import cn._51doit.udfs.GiftBroadCastProcessFunction;
import cn._51doit.udfs.JsonToDataBeanFunction;
import cn._51doit.utils.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 从Kafka中读取protobuf类型的二进制数据，并且将二进制数据转成protobuf的bean类型
*/
public class GiftCountFromProtoBuf {

    public static void main(String[] args) throws Exception {

        //注册DataBeanProto.DataBean使用ProtoBuf进行序列化
        FlinkUtilsV2.env.getConfig().registerTypeWithKryoSerializer(DataBeanProto.DataBean.class, PBSerializer.class);

        DataStream<DataBeanProto.DataBean> beanStream = FlinkUtilsV2.createKafkaStream(args[0], ProtoBufDeserializationSchema.class);

        beanStream.print();

        FlinkUtilsV2.env.execute();

    }
}
