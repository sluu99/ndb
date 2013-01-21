/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import java.io.IOException;

/**
 *
 * @author sluu
 */
public interface Record {
    
    public long getId();
    
    /**
     * Get the creation epoch timestamp 
     * @return 
     */
    public long getCreationTime();
    
    /**
     * Get the update epoch timestamp
     * @return 
     */
    public long getUpdateTime();
    
    /**
     * Get the previous record in the same table
     * @return 
     */
    public Record getPrevRecord() throws IOException, DataException;
    
    /**
     * Get the last n versions for the value of this key
     * @param n
     * @return
     * @throws IOException
     * @throws DataException 
     */
    public Value[] getValues(String key, int n) throws IOException, DataException;
    
    
    public void setByte(String key, byte value) throws IOException, DataException;
    public void setShort(String key, short value) throws IOException, DataException;
    public void setInt(String key, int value) throws IOException, DataException;
    public void setLong(String key, long value) throws IOException, DataException;
    public void setFloat(String key, float value) throws IOException, DataException;
    public void setDouble(String key, double value) throws IOException, DataException;
    public void setBool(String key, boolean value) throws IOException, DataException;    
    public void setString(String key, String value) throws IOException, DataException;
    public void setBigString(String key, String value) throws IOException, DataException;
    public void setBin(String key, byte[] value) throws IOException, DataException;
    
    public Value getValue(String key) throws IOException, DataException;
    public DataType getType(String key) throws IOException, DataException;
    public byte[] getRaw(String key) throws IOException, DataException;
    public long getTimestamp(String key) throws IOException, DataException;
    
    public byte getByte(String key) throws IOException, DataException;
    public short getShort(String key) throws IOException, DataException;
    public int getInt(String key) throws IOException, DataException;
    public long getLong(String key) throws IOException, DataException;
    public float getFloat(String key) throws IOException, DataException;
    public double getDouble(String key) throws IOException, DataException;
    public boolean getBool(String key) throws IOException, DataException;    
    public String getString(String key) throws IOException, DataException;
    public byte[] getBin(String key) throws IOException, DataException;
}
