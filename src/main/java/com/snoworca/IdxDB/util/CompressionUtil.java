package com.snoworca.IdxDB.util;

import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtil {

    // 바이트 배열 입력. Gzip 으로 압축. 압축된 바이트 배열 반환.
    public static byte[] compressGZIP(byte[] data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = null;
        try {
            gzipOutputStream = new GZIPOutputStream(baos);
            gzipOutputStream.write(data);
            gzipOutputStream.finish();
            gzipOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }


    public static byte[] decompressGZIP(byte[] data, int offset, int len) {
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
    public static byte[] compressDeflate(byte[] data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream deflaterOutputStream = null;
        try {
            deflaterOutputStream = new DeflaterOutputStream(baos);
            deflaterOutputStream.write(data);
            deflaterOutputStream.flush();
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    // Deflater  압축해제 메서드.
    public static byte[] decompressDeflate(byte[] data, int offset, int len) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            DeflaterInputStream gzipInStream = new DeflaterInputStream(new ByteArrayInputStream(data, offset, len));
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

    public static byte[] compressSnappy(byte[] data) {
        try {
            return Snappy.compress(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Deflater  압축해제 메서드.
    public static byte[] decompressSnappy(byte[] data, int offset, int len) {
        try {
            return Snappy.uncompress(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
