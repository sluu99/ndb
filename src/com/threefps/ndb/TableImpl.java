/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.errors.NotFoundException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Implementation of the Table interface
 *
 * @author sluu
 */
class TableImpl implements Table {

    private static final byte CURRENT_VERSION = 1;
    FileChannel file = null;
    TableHeaderImpl header = new TableHeaderImpl();

    // <editor-fold defaultstate="collapsed" desc="Getters and Setters">  
    private void setFile(FileChannel file) {
        this.file = file;
    }

    private FileChannel getFile() {
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
        table.setFile(FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE));
        table.getHeader().setFile(table.getFile());

        if (fileExists) {
            table.getHeader().read();
        } else {
            table.getHeader().setName(name);
            table.getHeader().setVersion(TableImpl.CURRENT_VERSION);
            table.getHeader().write();
            table.getFile().force(false);
        }
        return table;
    }

    /**
     * Close this table and files associated with it
     */
    @Override
    public void close() throws IOException {
        FileChannel f = getFile();
        if (f != null && f.isOpen()) {
            f.force(true);
            f.close();
        }
    }

    @Override
    public RecordImpl createRecord() throws IOException, DataException {
        TableHeaderImpl h = getHeader();
        RecordImpl r = RecordImpl.create(getFile(), h.getNewestRecordPos());
        h.setNewestRecordPos(r.getPos());
        h.incCount();
        h.writeNewestRecord();
        h.writeRecordCount();
        getFile().force(false);
        return r;
    }

    @Override
    public Record getRecord(int id) throws NotFoundException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
