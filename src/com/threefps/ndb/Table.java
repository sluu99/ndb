/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.NotFoundException;

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
    
    /**
     * Create a new record
     * @return 
     */
    public Record newRecord();
    
    /**
     * Get a record based on its ID number
     * @param id
     * @return 
     */
    public Record getRecord(int id) throws NotFoundException ;
}
