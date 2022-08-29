package com.snoworca.StreamDB.serialization;

import java.nio.ByteBuffer;

public interface DataSerializable {
    public long getVersion();
    public long getType();
    public void deserialize(Deserializer deserializer);
    public void serialize(Serializer serializer) throws Exception;
    public void migrate(long previousVersion, ByteBuffer previousBuffer, long newVersion, ByteBuffer newBuffer) throws Exception;

}
