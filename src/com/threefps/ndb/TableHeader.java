/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

/**
 *
 * @author sluu
 */
public interface TableHeader {
    
    /**
     * Get the table version
     * @return 
     */
    public byte getVersion();
    
    /**
     * Get the number of records in this table
     * @return 
     */
    public long getRecordCount();
    
    /**
     * Get the table name
     * @return 
     */
    public String getName();
}
