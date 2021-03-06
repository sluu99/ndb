/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.utils;

import com.threefps.ndb.Const;
import static java.lang.System.arraycopy;
import java.nio.ByteBuffer;

/**
 * A class that convert things to and from byte arrays
 * @author sluu
 */
public final class B {
    
    /**
     * Convert a byte to a byte array
     * @param b
     * @return An array with length of 1
     */
    public static byte[] fromByte(byte b) {
        byte[] bs = new byte[1];
        bs[0] = b;
        return bs;
    }
    
    /**
     * Convert a short to a byte array
     * @param s
     * @return 
     */
    public static byte[] fromShort(short s) {
        ByteBuffer buff = ByteBuffer.allocate(2);
        buff.putShort(s);
        return buff.array();
    }
    
    /**
     * Convert an int to a byte array
     * @param i
     * @return 
     */
    public static byte[] fromInt(int i) {
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(i);
        return buff.array();
    }
    
    /**
     * Convert a long to a byte array
     * @param l
     * @return 
     */
    public static byte[] fromLong(long l) {
        ByteBuffer buff = ByteBuffer.allocate(8);
        buff.putLong(l);
        return buff.array();
    }
    
    /**
     * Convert a float to a byte array
     * @param f
     * @return 
     */
    public static byte[] fromFloat(float f) {
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putFloat(f);
        return buff.array();
    }
    
    /**
     * Convert a double to a byte array
     * @param d
     * @return 
     */
    public static byte[] fromDouble(double d) {
        ByteBuffer buff = ByteBuffer.allocate(8);
        buff.putDouble(d);
        return buff.array();
    }
    
    /**
     * Convert a boolean to a byte array
     * @param b
     * @return 
     */
    public static byte[] fromBool(boolean b) {
        return b? fromByte((byte)1) : fromByte((byte)0);
    }
    
    /**
     * Create a byte array for String. Its size must fit into a byte.
     * @param s
     * @return A byte array. The first byte is the length. The rest is the string
     */
    public static byte[] fromString(String s) {
        byte[] sBytes = s.getBytes(Const.CHARSET);
        byte len = (byte)sBytes.length;
        byte[] b = new byte[len+1];
        arraycopy(sBytes, 0, b, 1, len);
        b[0] = len;
        return b;
    }
    
    /**
     * Create a byte array for String. Its size must fit into a byte.
     * @param s
     * @return A byte array. The first byte is the length. The rest is the string
     */
    public static byte[] fromBigString(String s) {
        byte[] sBytes = s.getBytes(Const.CHARSET);
        int len = sBytes.length;
        byte[] b = new byte[len+4];
        arraycopy(sBytes, 0, b, 4, len);
        arraycopy(B.fromInt(len), 0, b, 0, 4);
        return b;
    }
    
    /**
     * Create a byte array from another byte array.
     * The first four bytes will be length of the original data.
     * @param src
     * @return 
     */
    public static byte[] fromBin(byte[] src) {
        byte[] b = new byte[src.length + 4];
        arraycopy(fromInt(src.length), 0, b, 0, 4);
        arraycopy(src, 0, b, 4, src.length);
        return b;
    }
    
    /**
     * Get a short out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static short toShort(byte[] b, int offset) {
        return ByteBuffer.wrap(b).getShort(offset);
    }
    
    /**
     * Get an int out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static int toInt(byte[] b, int offset) {
        return ByteBuffer.wrap(b).getInt(offset);        
    }
    
    /**
     * Get a long out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static long toLong(byte[] b, int offset) {
        return ByteBuffer.wrap(b).getLong(offset);
    }
    
    /**
     * Get a float out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static float toFloat(byte[] b, int offset) {
        return ByteBuffer.wrap(b).getFloat(offset);
    }
    
    /**
     * Get a double out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static double toDouble(byte[] b, int offset) {
        return ByteBuffer.wrap(b).getDouble(offset);
    }
    
    /**
     * Get a boolean out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static boolean toBool(byte[] b, int offset) {
        return b[offset] == 1;
    }
}
