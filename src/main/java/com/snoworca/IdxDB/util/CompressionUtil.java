package com.snoworca.IdxDB.util;

import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.*;

public class CompressionUtil {

    // 바이트 배열 입력. Gzip 으로 압축. 압축된 바이트 배열 반환.
    public static byte[] compressGZIP(byte[] data, boolean writeLen) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = null;
        try {
            if(writeLen) {
                baos.write(new byte[4]);
            }
            gzipOutputStream = new GZIPOutputStream(baos);
            gzipOutputStream.write(data);
            gzipOutputStream.finish();
            gzipOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] buffer = baos.toByteArray();
        if(writeLen) {
            NumberBufferConverter.fromInt(buffer.length - 4, buffer, 0);
        }
        return buffer;
    }


    public static byte[] decompressGZIP(byte[] data, int offset, int len,boolean readLen) {
        if(readLen) {
            len = NumberBufferConverter.toInt(data, offset);
            offset += 4;
        }
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {

            GZIPInputStream gzipInStream = new GZIPInputStream(new ByteArrayInputStream(data, offset, len));
            int size = 0;
            byte[] buffer = new byte[1024];
            while ( (size = gzipInStream.read(buffer)) > 0 ) {
                outStream.write(buffer, 0, size);
            }
            outStream.flush();
            outStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return outStream.toByteArray();
    }

    // 바이트 배열을 입력받아 Deflater 로 압축된 바이트 배열을 반환하는 메서드.
    public static byte[] compressDeflate(byte[] data, boolean writeLen) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream deflaterOutputStream = null;
        try {
            if(writeLen) {
                baos.write(new byte[4]);
            }
            deflaterOutputStream = new DeflaterOutputStream(baos);
            deflaterOutputStream.write(data);
            deflaterOutputStream.flush();
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] buffer = baos.toByteArray();
        if(writeLen) {
            NumberBufferConverter.fromInt(buffer.length - 4, buffer, 0);
        }
        return buffer;
    }

    // Deflater  압축해제 메서드.
    public static byte[] decompressDeflate(byte[] data, int offset, int len,boolean readLen) {
        if(readLen) {
            len = NumberBufferConverter.toInt(data, offset);
            offset += 4;
        }
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            Inflater inflater = new Inflater();
            inflater.setInput(data, offset, len);
            int size = 0;
            byte[] buffer = new byte[4096];
            while(!inflater.finished()) {
                size = inflater.inflate(buffer);
                outStream.write(buffer, 0, size);
            }
            outStream.flush();
            outStream.close();
        } catch (IOException | DataFormatException e) {
            throw new RuntimeException(e);
        }
        return outStream.toByteArray();
    }

    public static byte[] compressSnappy(byte[] data,boolean writeLen) {
        try {
            if(writeLen) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write(new byte[4]);
                byte[] buffer = Snappy.compress(data);
                baos.write(buffer);
                buffer = baos.toByteArray();
                NumberBufferConverter.fromInt(buffer.length - 4, buffer, 0);
                return buffer;
            }
            return Snappy.compress(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    // Deflater  압축해제 메서드.
    public static byte[] decompressSnappy(byte[] data, int offset, int len,boolean readLen ) {
        try {
            if(readLen) {
                int length = NumberBufferConverter.toInt(data, offset);
                byte[] array = new byte[length];
                System.arraycopy(data, offset + 4, array, 0, length);
                return Snappy.uncompress(array);
            }
            return Snappy.uncompress(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
