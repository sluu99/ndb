/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import java.io.IOException;
import java.util.HashMap;

/**
 * The main class for NDB
 * @author sluu
 */
public class NDB {
    
    private String dir = null;
    private HashMap<String, TableImpl> tables = new HashMap<>();
    
    
    /**
     * Create a new instance of NDB
     * @param dir The data directory
     */
    public NDB(String dir) throws RuntimeException {
        if (dir == null || dir.trim().equals("")) {
            throw new RuntimeException("Data directory cannot be empty");
        } else {
            this.dir = dir;
        }
    }
    
    /**
     * Get a table instance from the table name.
     * If the table data files do not exist, they will be created.
     * @param name The table name (case insensitive)
     * @return 
     */
    public Table getTable(String name) throws IOException, DataException {
        name = name.toLowerCase().trim();
        if (tables.containsKey(name)) {
            return tables.get(name);
        }
        
        TableImpl t = TableImpl.open(name, dir);
        tables.put(name, t);
        return t;
    }
 
    public void close() throws IOException {
        for (TableImpl t: tables.values()) {
            t.close();
        }
    }
    
}
