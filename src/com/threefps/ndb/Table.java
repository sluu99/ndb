/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.errors.LimitException;
import com.threefps.ndb.errors.NotFoundException;
import java.io.IOException;

/**
 * It's an interface for a data table.
 * @author sluu
 */
public interface Table {
    /**
     * Get the current file version of the table.
     * @return 
     */
    public byte getVersion();
    
    /**
     * Get the table name
     * @return 
     */
    public String getName();
    
    public FieldInfo getField() throws LimitException;
    
    /**
     * Create a new record
     * @return 
     */
    public Record newRecord() throws IOException, DataException;
    
    /**
     * Get a record based on its ID number
     * @param id
     * @return 
     */
    public Record getRecord(int id) throws NotFoundException, DataException;
}
