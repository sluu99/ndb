/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

/**
 *
 * @author sluu
 */
public interface Record {
    
    public long getId();
    
    /**
     * Get the creation epoch timestamp 
     * @return 
     */
    public long getCreationTime();
    
    /**
     * Get the update epoch timestamp
     * @return 
     */
    public long getUpdateTime();
    
    /**
     * Get the previous record in the same table
     * @return 
     */
    public Record getPrevRecord();
}
