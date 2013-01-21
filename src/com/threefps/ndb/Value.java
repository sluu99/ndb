/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;

/**
 *
 * @author sluu
 */
public interface Value {
    
    /**
     * Get the timestamp of this value
     * @return 
     */
    public long getTimestamp();
    
    /**
     * Get the data type of this value
     * @return 
     */
    public DataType getType();
    
    /**
     * The raw data for this value
     * @return 
     */
    public byte[] raw();
    
    public byte asByte() throws DataException;
    public short asShort() throws DataException;
    public int asInt() throws DataException;
    public long asLong() throws DataException;
    public float asFloat() throws DataException;
    public double asDouble() throws DataException;
    public boolean asBool() throws DataException;    
    public String asString() throws DataException;
    public byte[] asBin() throws DataException;
}
