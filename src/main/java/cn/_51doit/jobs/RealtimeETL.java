package cn._51doit.jobs;

import akka.remote.serialization.ProtobufSerializer;
import cn._51doit.proto.DataBeanProto;
import cn._51doit.udfs.JsonToProtoBufFunction;
import cn._51doit.utils.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * 对Kafka中的原始数据进行ETL预处理
 * 1.将Kafka中的数据读取过来
 * 2.将数据转换（json字符串转成protobuf bean类型）
 * 3.过滤有问题的数据
 * 4.将数据按照主题拆分（侧流输出）
 * 5.将数据转成protobuf的二进制，保存到Kafka中（节省空间）
 */
public class RealtimeETL {


    public static void main(String[] args) throws Exception{


        //Flink使用的是kryo的序列化方式
        //修改Flink对某些bean的序列化方式，DataBeanProto.DataBean改成PortoBuf
        FlinkUtilsV2.env.getConfig().registerTypeWithKryoSerializer(DataBeanProto.DataBean.class, PBSerializer.class);

        DataStream<Tuple2<String, String>> kafkaStreamWithId = FlinkUtilsV2.createKafkaStreamWithId(args[0], MyKafkaDeserializationSchema.class);

        SingleOutputStreamOperator<DataBeanProto.DataBean> beanStream = kafkaStreamWithId.process(new JsonToProtoBufFunction());

        OutputTag<DataBeanProto.DataBean> productTag= new OutputTag<DataBeanProto.DataBean>("product-tag") {
        };

        OutputTag<DataBeanProto.DataBean> giftTag  = new OutputTag<DataBeanProto.DataBean>("gift-tag") {
        };

        //将数据打上标签
        SingleOutputStreamOperator<DataBeanProto.DataBean> mainStream = beanStream.process(new ProcessFunction<DataBeanProto.DataBean, DataBeanProto.DataBean>() {

            @Override
            public void processElement(DataBeanProto.DataBean bean, Context ctx, Collector<DataBeanProto.DataBean> out) throws Exception {

                String eventId = bean.getEventId();
                if (eventId.startsWith("product")) {
                    ctx.output(productTag, bean);
                }
                if (Constants.LIVE_REWARD.equals(eventId)) {
                    ctx.output(giftTag, bean);
                }
                //输出全部的的数据
                //out.collect(bean);
            }

        });


        DataStream<DataBeanProto.DataBean> productStream = mainStream.getSideOutput(productTag);

        DataStream<DataBeanProto.DataBean> giftStream = mainStream.getSideOutput(giftTag);

        //将拆分的后数据流写入到Kafka中

        Properties properties = new Properties();
        String bootstrapServers = FlinkUtilsV2.parameterTool.getRequired("bootstrap.servers");
        properties.setProperty("bootstrap.servers", bootstrapServers);
        String productTopic = FlinkUtilsV2.parameterTool.getRequired("kafka.product.output");
        String giftTopic = FlinkUtilsV2.parameterTool.getRequired("kafka.gift.output");
        FlinkKafkaProducer<DataBeanProto.DataBean> productKafkaProducer = new FlinkKafkaProducer<>(
                productTopic,
                new KafkaProtoBufSerializationSchema(productTopic),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        productStream.addSink(productKafkaProducer);


        FlinkKafkaProducer<DataBeanProto.DataBean> giftKafkaProducer = new FlinkKafkaProducer<>(
                giftTopic,
                new KafkaProtoBufSerializationSchema(giftTopic),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        giftStream.addSink(giftKafkaProducer);

        FlinkUtilsV2.env.execute();

    }



}
