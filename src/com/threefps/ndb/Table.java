/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import java.io.IOException;

/**
 * It's an interface for a data table.
 * @author sluu
 */
public interface Table {
    
    /**
     * Get this header information of this table
     * @return 
     */
    public TableHeader getHeader();
    
    /**
     * Create a new record
     * @return 
     */
    public Record createRecord() throws IOException, DataException;
    
    /**
     * Get a record based on its ID number
     * @param id
     * @return 
     */
    public Record getRecord(long id) throws IOException, DataException;
    
    /**
     * Get the last created record
     * @return
     * @throws IOException
     * @throws DataException 
     */
    public Record getLastRecord() throws IOException, DataException;
    
    /**
     * Close the table and all the files associated with it
     */
    public void close() throws IOException;
}
