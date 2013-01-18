/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

/**
 *
 * @author sluu
 */
public interface Record {
    
    public long getId();
    
    public byte getByte(String key);
    public short getShort(String key);
    public int getInt(String key);
    public long getLong(String key);
    public float getFloat(String key);
    public double getDouble(String key);
    public boolean getBool(String key);
    public String getString(String key);
    
    public void setByte(String key, byte value);
    public void setShort(String key, short value);
    public void setInt(String key, int value);
    public void setLong(String key, long value);
    public void setFloat(String key, float value);
    public void setDouble(String key, double value);
    public void setBool(String key, boolean value);
    public void setString(String key, String value);
}
