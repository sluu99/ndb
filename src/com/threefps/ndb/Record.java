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
    
    
    public void setString(String key, String value) throws IOException, DataException;
}
