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
    
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public byte asByte() throws DataException;
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public short asShort() throws DataException;
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public int asInt() throws DataException;
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public long asLong() throws DataException;
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public float asFloat() throws DataException;
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public double asDouble() throws DataException;
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public boolean asBool() throws DataException;    
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public String asString() throws DataException;
    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    public byte[] asBin() throws DataException;
}
