package cn._51doit.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.Message;

import java.lang.reflect.Method;
import java.util.HashMap;

public class PBSerializer extends Serializer<Message> {

        /* This cache never clears, but only scales like the number of
         * classes in play, which should not be very large.
         * We can replace with a LRU if we start to see any issues.
         */
        final protected HashMap<Class, Method> methodCache = new HashMap<Class, Method>();

        /**
         * This is slow, so we should cache to avoid killing perf:
         * See: http://www.jguru.com/faq/view.jsp?EID=246569
         */
        protected Method getParse(Class cls) throws Exception {
            Method meth = methodCache.get(cls);
            if (null == meth) {
                meth = cls.getMethod("parseFrom", new Class[]{ byte[].class });
                methodCache.put(cls, meth);
            }
            return meth;
        }

        //序列化（将数据转成protobuf二进制）
        @Override
        public void write(Kryo kryo, Output output, Message mes) {
            byte[] ser = mes.toByteArray();
            output.writeInt(ser.length, true);
            output.writeBytes(ser);
        }

        //反序列化（将二进制转成protobuf类型）
        @Override
        public Message read(Kryo kryo, Input input, Class<Message> pbClass) {
            try {
                int size = input.readInt(true);
                byte[] barr = new byte[size];
                input.readBytes(barr);
                return (Message)getParse(pbClass).invoke(null, barr);
            } catch (Exception e) {
                throw new RuntimeException("Could not create " + pbClass, e);
            }
        }
    }

