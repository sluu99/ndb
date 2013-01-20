/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

/**
 * Field data types
 * @author sluu
 */
public enum DataType {
    NONE,
    BYTE,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOL,
    STRING,
    BIG_STRING;
    
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
        }
        return NONE;
    }
    
    public byte toByte() {
        switch (this) {
            case BYTE: return 1;
            case SHORT: return 2;
            case INT: return 3;
            case LONG: return 4;
            case FLOAT: return 5;
            case DOUBLE: return 6;
            case BOOL: return 7;
            case STRING: return 8;
            case BIG_STRING: return 9;
        }
        return 0;
    }
    
    /**
     * Return the size for this data type. Except for STRING, which will return 0
     * @return 
     */
    public byte size() {
        switch (this) {
            case BYTE: return 1;
            case SHORT: return 2;
            case INT: return 4;
            case LONG: return 8;
            case FLOAT: return 4;
            case DOUBLE: return 8;
            case BOOL: return 1;            
        }
        return 0;
    }
}
