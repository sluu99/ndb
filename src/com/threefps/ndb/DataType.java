/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

/**
 * Field data types
 * @author sluu
 */
public enum DataType {
    /**
     * Does not represent any data type
     */
    NONE,
    /**
     * 8 bit number
     */
    BYTE,
    /**
     * 16 bite number
     */
    SHORT,
    /**
     * 32 bit integer
     */
    INT,
    /**
     * 64 bit integer
     */
    LONG,
    /**
     * 32 bit float
     */
    FLOAT,
    /**
     * 64 bit double
     */
    DOUBLE,
    /**
     * Boolean
     */
    BOOL,
    /**
     * String with length less than 128
     */
    STRING,
    /**
     * String with length > 128 or more
     */
    BIG_STRING,
    /**
     * Just raw binary
     */
    BINARY;
    
    /**
     * Get a data type from its byte representation
     * @param b
     * @return 
     */
    public static DataType fromByte(byte b) {
        switch (b) {
            case 1: return BYTE;
            case 2: return SHORT;
            case 3: return INT;
            case 4: return LONG;
            case 5: return FLOAT;
            case 6: return DOUBLE;
            case 7: return BOOL;
            case 8: return STRING;
            case 9: return BIG_STRING;
            case 10: return BINARY;
        }
        return NONE;
    }
    
    /**
     * Get byte representation of the data type
     * @return 
     */
    public byte toByte() {        
        if (this == BYTE) return 1;
        if (this == SHORT) return 2;
        if (this == INT) return 3;
        if (this == LONG) return 4;
        if (this == FLOAT) return 5;
        if (this == DOUBLE) return 6;
        if (this == BOOL) return 7;
        if (this == STRING) return 8;
        if (this == BIG_STRING) return 9;
        if (this == BINARY) return 10;
        
        return 0;
    }
    
    /**
     * Return the size for this data type. Except for STRING, which will return 0
     * @return 
     */
    public int size() {
        
        if (this == BYTE) return 1;
        if (this == SHORT) return 2;
        if (this == INT) return 4;
        if (this == LONG) return 8;
        if (this == FLOAT) return 4;
        if (this == DOUBLE) return 8;
        if (this == BOOL) return 1;   
        if (this == STRING) return 1;
        if (this == BIG_STRING) return 4;
        if (this == BINARY) return 4;
        
        return 0;
    }
    
    /**
     * Get the data type's rank
     * @return 
     */
    public int rank() {
        if (this == BOOL) return 20;
        if (this == STRING || this == BIG_STRING) return 30;
        if (this == BINARY) return 40;
        return 10;
    }
}
