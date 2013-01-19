/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.utils;

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
     * Get a long out of a byte array
     * @param b
     * @param offset
     * @return 
     */
    public static long toLong(byte[] b, int offset) {
        return ByteBuffer.wrap(b, offset, 8).getLong();        
    }
}
