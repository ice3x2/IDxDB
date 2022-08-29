package com.snoworca.IDxDB.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteBufferOutputStream extends OutputStream {

        protected ByteBuffer byteBuffer;
        protected byte[] buffer;

        /**
         * The number of valid bytes in the buffer.
         */
        protected int count;

        /**
         * Creates a new byte array output stream. The buffer capacity is
         * initially 32 bytes, though its size increases if necessary.
         */
        public ByteBufferOutputStream() {
            this(1024);
        }

        /**
         * Creates a new byte array output stream, with a buffer capacity of
         * the specified size, in bytes.
         *
         * @param   size   the initial size.
         * @exception  IllegalArgumentException if size is negative.
         */
        public ByteBufferOutputStream(int size) {
            if (size < 0) {
                throw new IllegalArgumentException("Negative initial size: "
                        + size);
            }
            buffer = new byte[size];
            byteBuffer = ByteBuffer.wrap(buffer);
        }

        /**
         * Increases the capacity if necessary to ensure that it can hold
         * at least the number of elements specified by the minimum
         * capacity argument.
         *
         * @param minCapacity the desired minimum capacity
         * @throws OutOfMemoryError if {@code minCapacity < 0}.  This is
         * interpreted as a request for the unsatisfiably large capacity
         * {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}.
         */
        private void ensureCapacity(int minCapacity) {
            // overflow-conscious code
            if (minCapacity - buffer.length > 0)
                grow(minCapacity);
        }

        /**
         * The maximum size of array to allocate.
         * Some VMs reserve some header words in an array.
         * Attempts to allocate larger arrays may result in
         * OutOfMemoryError: Requested array size exceeds VM limit
         */
        private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

        /**
         * Increases the capacity to ensure that it can hold at least the
         * number of elements specified by the minimum capacity argument.
         *
         * @param minCapacity the desired minimum capacity
         */
        private void grow(int minCapacity) {
            // overflow-conscious code
            int oldCapacity = buffer.length;
            int newCapacity = oldCapacity << 1;
            if (newCapacity - minCapacity < 0)
                newCapacity = minCapacity;
            if (newCapacity - MAX_ARRAY_SIZE > 0)
                newCapacity = hugeCapacity(minCapacity);
            buffer = Arrays.copyOf(buffer, newCapacity);
            byteBuffer = ByteBuffer.wrap(buffer);
            byteBuffer.limit(newCapacity);
            byteBuffer.position(count);


        }


        private static int hugeCapacity(int minCapacity) {
            if (minCapacity < 0) // overflow
                throw new OutOfMemoryError();
            return (minCapacity > MAX_ARRAY_SIZE) ?
                    Integer.MAX_VALUE :
                    MAX_ARRAY_SIZE;
        }

        /**
         * Writes the specified byte to this byte array output stream.
         *
         * @param   b   the byte to be written.
         */
        public synchronized void write(int b) {
            ensureCapacity(count + 1);
            byteBuffer.put((byte)b);
            count += 1;
        }

        /**
         * Writes <code>len</code> bytes from the specified byte array
         * starting at offset <code>off</code> to this byte array output stream.
         *
         * @param   b     the data.
         * @param   off   the start offset in the data.
         * @param   len   the number of bytes to write.
         */
        public synchronized void write(byte b[], int off, int len) {
            if ((off < 0) || (off > b.length) || (len < 0) ||
                    ((off + len) - b.length > 0)) {
                throw new IndexOutOfBoundsException();
            }
            ensureCapacity(count + len);
            byteBuffer.put(b, off, len);
            //System.arraycopy(b, off, buf, count, len);
            count += len;
        }


        public void writeInt(int value) {
            int len = 4;
            ensureCapacity(count + len);
            byteBuffer.putInt(value);
            count += len;
        }

        public void writeFloat(float value) {
            int len = 4;
            ensureCapacity(count + len);
            byteBuffer.putFloat(value);
            count += len;
        }

        public void writeShort(short value) {
            int len = 2;
            ensureCapacity(count + len);
            byteBuffer.putShort(value);
            count += len;
        }

        public void writeChar(char value) {
            int len = 2;
            ensureCapacity(count + len);
            byteBuffer.putChar(value);
            count += len;
        }

        public void writeLong(long value) {
            int len = 8;
            ensureCapacity(count + len);
            byteBuffer.putLong(value);
            count += len;
        }

        public void writeDouble(double value) {
            int len = 8;
            ensureCapacity(count + len);
            byteBuffer.putDouble(value);
            count += len;
        }


        /**
         * Writes the complete contents of this byte array output stream to
         * the specified output stream argument, as if by calling the output
         * stream's write method using <code>out.write(buf, 0, count)</code>.
         *
         * @param      out   the output stream to which to write the data.
         * @exception IOException  if an I/O error occurs.
         */
        public synchronized void writeTo(OutputStream out) throws IOException {
            out.write(buffer, 0, count);
        }

        /**
         * Resets the <code>count</code> field of this byte array output
         * stream to zero, so that all currently accumulated output in the
         * output stream is discarded. The output stream can be used again,
         * reusing the already allocated buffer space.
         *
         * @see     java.io.ByteArrayInputStream
         */
        public synchronized void reset() {
            count = 0;
        }

        /**
         * Creates a newly allocated byte array. Its size is the current
         * size of this output stream and the valid contents of the buffer
         * have been copied into it.
         *
         * @return  the current contents of this output stream, as a byte array.
         * @see     java.io.ByteArrayOutputStream#size()
         */
        public synchronized byte toByteArray()[] {
            return Arrays.copyOf(buffer, count);
        }

        /**
         * Returns the current size of the buffer.
         *
         * @return  the value of the <code>count</code> field, which is the number
         *          of valid bytes in this output stream.
         * @see     java.io.ByteArrayOutputStream
         */
        public synchronized int size() {
            return count;
        }

        /**
         * Converts the buffer's contents into a string decoding bytes using the
         * platform's default character set. The length of the new <tt>String</tt>
         * is a function of the character set, and hence may not be equal to the
         * size of the buffer.
         *
         * <p> This method always replaces malformed-input and unmappable-character
         * sequences with the default replacement string for the platform's
         * default character set. The {@linkplain java.nio.charset.CharsetDecoder}
         * class should be used when more control over the decoding process is
         * required.
         *
         * @return String decoded from the buffer's contents.
         * @since  JDK1.1
         */
        public synchronized String toString() {
            return new String(buffer, 0, count);
        }

        /**
         * Converts the buffer's contents into a string by decoding the bytes using
         * the named {@link java.nio.charset.Charset charset}. The length of the new
         * <tt>String</tt> is a function of the charset, and hence may not be equal
         * to the length of the byte array.
         *
         * <p> This method always replaces malformed-input and unmappable-character
         * sequences with this charset's default replacement string. The {@link
         * java.nio.charset.CharsetDecoder} class should be used when more control
         * over the decoding process is required.
         *
         * @param      charsetName  the name of a supported
         *             {@link java.nio.charset.Charset charset}
         * @return     String decoded from the buffer's contents.
         * @exception UnsupportedEncodingException
         *             If the named charset is not supported
         * @since      JDK1.1
         */
        public synchronized String toString(String charsetName)
                throws UnsupportedEncodingException
        {
            return new String(buffer, 0, count, charsetName);
        }

        /**
         * Creates a newly allocated string. Its size is the current size of
         * the output stream and the valid contents of the buffer have been
         * copied into it. Each character <i>c</i> in the resulting string is
         * constructed from the corresponding element <i>b</i> in the byte
         * array such that:
         * <blockquote><pre>
         *     c == (char)(((hibyte &amp; 0xff) &lt;&lt; 8) | (b &amp; 0xff))
         * </pre></blockquote>
         *
         * @deprecated This method does not properly convert bytes into characters.
         * As of JDK&nbsp;1.1, the preferred way to do this is via the
         * <code>toString(String enc)</code> method, which takes an encoding-name
         * argument, or the <code>toString()</code> method, which uses the
         * platform's default character encoding.
         *
         * @param      hibyte    the high byte of each resulting Unicode character.
         * @return     the current contents of the output stream, as a string.
         * @see        java.io.ByteArrayOutputStream#size()
         * @see        java.io.ByteArrayOutputStream#toString(String)
         * @see        java.io.ByteArrayOutputStream#toString()
         */
        @Deprecated
        public synchronized String toString(int hibyte) {
            return new String(buffer, hibyte, 0, count);
        }

        /**
         * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in
         * this class can be called after the stream has been closed without
         * generating an <tt>IOException</tt>.
         */
        public void close() throws IOException {
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

}
