/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.utils;

/**
 *
 * @author sluu
 */
public class Str {
    
    /**
     * Create a new string from a zero-delimited buffer.
     * The string will stop right before the first byte equals zero.
     * @param b
     * @param offset Position to start looking
     * @param len Number of characters to look at
     * @return A string, with length probably less than the input len
     */
    public static String fromCString(byte[] b, int offset, int len) {
        int x = offset;
        int end = offset + len;
        while (b[x] != 0 && x < end)
            x++;
        return new String (b, offset, x - offset);
    }
    
    /**
     * Shortcut for fromCString(b, 0, b.length)
     * @param b
     * @return 
     */
    public static String fromCString(byte[] b) {
       return fromCString(b, 0, b.length);
    }
    
    /**
     * Create a new buffer to contain the string.
     * The remaining bytes after the string will be zero.
     * @param s
     * @param len
     * @return 
     */
    public static byte[] toBuffer(String s, int len) {
        byte[] dst = new byte[len];
        byte[] src = s.getBytes();
        System.arraycopy(src, 0, dst, 0, src.length);
        while (len > src.length) {
            len--;
            dst[len] = 0;
        }
        return dst;
    }
    
}
