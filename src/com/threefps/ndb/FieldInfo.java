/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

/**
 *
 * @author sluu
 */
public class FieldInfo {
    
    private static final int MAX_NAME_LENGTH = 30;
    
    private byte number = 0;
    private String name = null;
    private DataType type;
    private int FDP = 0;
   

    FieldInfo(byte number, String name, DataType type, int FDP) {
        name = name.trim().toLowerCase();
        if (name.length() > MAX_NAME_LENGTH) name = name.substring(0, MAX_NAME_LENGTH);
        
        this.number = number;
        this.name = name;
        this.type = type;
        this.FDP = FDP;
    } 
    
    public byte getNumber() {
        return number;
    }
    
    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    /**
     * Get the First Data Position
     * @return 
     */
    public int getFDP() {
        return FDP;
    }
    
}
