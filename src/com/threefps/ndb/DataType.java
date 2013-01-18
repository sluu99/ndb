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
    NONE,
    BYTE,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    STRING;
    
    public static DataType fromByte(byte b) {
        switch (b) {
            case 1: return BYTE;
            case 2: return SHORT;
            case 3: return INT;
            case 4: return LONG;
            case 5: return FLOAT;
            case 6: return DOUBLE;
            case 7: return BOOLEAN;
            case 8: return STRING;
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
            case BOOLEAN: return 7;
            case STRING: return 8;
        }
        return 0;
    }
}
