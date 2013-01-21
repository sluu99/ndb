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
     *
     * @return
     */
    public long getCreationTime();

    /**
     * Get the update epoch timestamp
     *
     * @return
     */
    public long getUpdateTime();

    /**
     * Get the previous record in the same table
     *
     * @return
     */
    public Record getPrevRecord() throws IOException, DataException;

    /**
     * Get the last n versions for the value of this key
     *
     * @param n
     * @return
     * @throws IOException
     * @throws DataException
     */
    public Value[] getValues(String key, int n) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setByte(String key, byte value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setShort(String key, short value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setInt(String key, int value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setLong(String key, long value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setFloat(String key, float value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setDouble(String key, double value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setBool(String key, boolean value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setString(String key, String value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setBigString(String key, String value) throws IOException, DataException;

    /**
     * Set a new value
     *
     * @param key The key name
     * @param value The new value
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key needs to be created and the key size
     * exceeds the page size
     */
    public void setBin(String key, byte[] value) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public Value getValue(String key) throws IOException, DataException;

    /**
     * Get the data type for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public DataType getType(String key) throws IOException, DataException;

    /**
     * Get the raw value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public byte[] getRaw(String key) throws IOException, DataException;

    /**
     * Get the timestamp for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public long getTimestamp(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public byte getByte(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public short getShort(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public int getInt(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public long getLong(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public float getFloat(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public double getDouble(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public boolean getBool(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public String getString(String key) throws IOException, DataException;

    /**
     * Get the value for a specific key
     *
     * @param key Key name
     * @return The value
     * @throws IOException If an IO error occurred
     * @throws DataException If the key does not exist
     */
    public byte[] getBin(String key) throws IOException, DataException;
}
