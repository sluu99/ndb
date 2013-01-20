/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.utils;

import static java.lang.System.arraycopy;
import java.nio.ByteBuffer;

/**
 * A class that convert things to and from byte arrays
 * @author sluu
 */
public class B {
    
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
     * Convert an int to a byte array
     * @param i
     * @return 
     */
    public static byte[] fromInt(int i) {
        ByteBuffer buff = ByteBuffer.allocate(i);
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
     * Create a byte array for String. Its size must fit into a byte.
     * @param s
     * @return A byte array. The first byte is the length. The rest is the string
     */
    public static byte[] fromSmallString(String s) {
        byte[] sBytes = s.getBytes();
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
    public static byte[] fromString(String s) {
        byte[] sBytes = s.getBytes();
        int len = sBytes.length;
        byte[] b = new byte[len+4];
        arraycopy(sBytes, 0, b, 4, len);
        arraycopy(B.fromInt(len), 0, b, 0, 4);
        return b;
    }
    
    /**
     * Get a long out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static long toLong(byte[] b, int offset) {
        return ByteBuffer.wrap(b, offset, 8).getLong();        
    }
}
