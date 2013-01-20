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
    public Record getPrevRecord();
    
    
    public void setByte(String key, byte value) throws IOException, DataException;
    public void setShort(String key, short value) throws IOException, DataException;
    public void setInt(String key, int value) throws IOException, DataException;
    public void setLong(String key, long value) throws IOException, DataException;
    public void setFloat(String key, float value) throws IOException, DataException;
    public void setDouble(String key, double value) throws IOException, DataException;
    public void setBoolean(String key, boolean value) throws IOException, DataException;    
    public void setString(String key, String value) throws IOException, DataException;
    public void setBigString(String key, String value) throws IOException, DataException;
    public void setBin(String key, byte[] value) throws IOException, DataException;
}
