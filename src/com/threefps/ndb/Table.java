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
    public Record getRecord(int id) throws NotFoundException, DataException;
    
    /**
     * Close the table and all the files associated with it
     */
    public void close() throws IOException;
}
