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
    
    public long getTimestamp();
    public DataType getType();
    
    public byte asByte() throws DataException;
    public short asShort() throws DataException;
    public int asInt() throws DataException;
    public long asLong() throws DataException;
    public float asFloat() throws DataException;
    public double asDouble() throws DataException;
    public boolean asBool() throws DataException;    
    public String asString() throws DataException;
    public byte[] asBin() throws DataException;
    
    public byte[] raw();
}
