/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.Record;
import com.threefps.ndb.Table;
import com.threefps.ndb.errors.DataException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * Implementation of the Table interface
 *
 * @author sluu
 */
public class TableImpl implements Table {

    private static final byte CURRENT_VERSION = 1;
    DataFile file = null;
    TableHeaderImpl header = new TableHeaderImpl();

    // <editor-fold defaultstate="collapsed" desc="Getters and Setters">  
    private void setFile(DataFile file) {
        this.file = file;
    }

    public DataFile getFile() {
        return file;
    }

    @Override
    public TableHeaderImpl getHeader() {
        return header;
    }

    private void setHeader(TableHeaderImpl header) {
        this.header = header;
    }
    // </editor-fold>

    /**
     * Create or open a new
     *
     * @param name
     * @param dir
     * @param create
     * @return
     */
    public static TableImpl open(String name, String dir) throws IOException, DataException {
        name = name.trim().toLowerCase();

        Path path = FileSystems.getDefault().getPath(dir, name + ".tbl");

        File file = path.toFile();
        boolean fileExists = file.exists();
        if (!fileExists) {
            file.createNewFile();
        }

        TableImpl table = new TableImpl();
        DataFile f = new DataFile(path);
        table.setFile(f);
        table.getHeader();

        if (fileExists) {
            table.getHeader().read(f);
        } else {
            table.getHeader().setName(name);
            table.getHeader().setVersion(TableImpl.CURRENT_VERSION);
            table.getHeader().create(f);
        }
        return table;
    }

    /**
     * Close this table and files associated with it
     */
    @Override
    public void close() throws IOException {
        if (getFile() != null) getFile().close();;
    }

    @Override
    public RecordImpl createRecord() throws IOException, DataException {
        TableHeaderImpl h = getHeader();
        RecordImpl r = RecordImpl.create(this, h.getNewestRecordPos());
        h.setNewestRecordPos(r.getPos());
        h.incCount();
        DataFile f = getFile();
        h.writeNewestRecord(f);
        h.writeRecordCount(f);
        return r;
    }

    @Override
    public Record getRecord(long id) throws DataException, IOException {
        return RecordImpl.read(this, id);
    }

    @Override
    public Record getLastRecord() throws IOException, DataException {
        return RecordImpl.read(this, getHeader().getNewestRecordPos());
    }
    
    
}
