/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

/**
 *
 * @author sluu
 */
class DataRecord implements Record {

    private long id = 0;
    
    void setId(long id) {
        this.id = id;
    }
    
    @Override
    public long getId() {
        return id;
    }

    @Override
    public byte getByte(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public short getShort(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getInt(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getLong(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float getFloat(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public double getDouble(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean getBool(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getString(String key) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setByte(String key, byte value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setShort(String key, short value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setInt(String key, int value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setLong(String key, long value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFloat(String key, float value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setDouble(String key, double value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setBool(String key, boolean value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setString(String key, String value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
