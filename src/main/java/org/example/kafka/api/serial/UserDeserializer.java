package org.example.kafka.api.serial;

import org.apache.kafka.common.serialization.Deserializer;
import org.example.kafka.api.User;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class UserDeserializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public User deserialize(String topic, byte[] data) {
        User user = new User();
        // 如果id和name都为空 data也可能不为空 因为还有4 + 4 = 8字节的头信息
        try {
            if (null != data && data.length >= 8) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                int idLen = buffer.getInt();
                byte[] idBytes = new byte[idLen];
                buffer.get(idBytes);

                int nameLen = buffer.getInt();
                byte[] nameBytes = new byte[nameLen];
                buffer.get(nameBytes);

                String id = new String(idBytes,"UTF-8");
                String name = new String(nameBytes, "UTF-8");
                user.setId(id);
                user.setName(name);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return user;
    }

    @Override
    public void close() {

    }
}
