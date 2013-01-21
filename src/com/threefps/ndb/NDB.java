/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.impl.TableImpl;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;

/**
 * The main class for NDB
 * @author sluu
 */
public class NDB implements Closeable {
    
    /**
     * Data directory
     */
    private String dir = null;
    /**
     * A map between table names and their actual table objects
     */
    private HashMap<String, TableImpl> tables = new HashMap<>();
    
    
    /**
     * Create a new instance of NDB
     * @param dir The data directory
     * @throws RuntimeException If the directory is empty
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
     * 
     * @param name The table name (case insensitive)     
     * @return The table instance
     * @throws IOException If an I/O error occurred
     * @throws DataException If the header size is larger than the underlying page size while creating the table
     */
    public Table getTable(String name) throws IOException, DataException {
        name = name.toLowerCase(Locale.getDefault()) .trim();
        if (tables.containsKey(name)) {
            return tables.get(name);
        }
        
        TableImpl t = TableImpl.open(name, dir);
        tables.put(name, t);
        return t;
    }
 
    /**
     * Closes the data base and all the tables
     * @throws IOException If an I/O error occurred
     */
    @Override
    public void close() throws IOException {
        for (TableImpl t: tables.values()) {
            t.close();
        }
    }
    
}
