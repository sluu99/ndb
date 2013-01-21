/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import java.nio.charset.Charset;

/**
 * Constants
 * @author sluu
 */
public class Const {
    
    /**
     * Size of each data type
     */
    public static final int DATA_TYPE_SIZE = 1; 
    
    /**
     * Size of pointer to a file position
     */    
    public static final int POINTER_SIZE = 8;
    
    /**
     * Size of timestamp
     */
    public static final int TIMESTAMP_SIZE = 8;
    
    /**
     * UTF-8 charset
     */
    public static final Charset CHARSET = Charset.forName("UTF-8");
}
