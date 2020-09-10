package org.example.kafka.api.serial;

import org.apache.kafka.common.serialization.Serializer;
import org.example.kafka.api.User;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义user对象序列化器
 */
public class UserSerializer implements Serializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, User user) {
        String id = user.getId();
        String name = user.getName();

        byte[] idBytes;
        byte[] nameBytes;

        try {
            if (null != id) {
                idBytes = id.getBytes("UTF-8");
            }else {
                idBytes = new byte[0];
            }

            if (null != name && !name.equals("")) {
                nameBytes = name.getBytes("UTF-8");
            }else {
                nameBytes = new byte[0];
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + idBytes.length + 4 + nameBytes.length);
            byteBuffer.putInt(idBytes.length);
            byteBuffer.put(idBytes);
            byteBuffer.putInt(nameBytes.length);
            byteBuffer.put(nameBytes);

            return byteBuffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
