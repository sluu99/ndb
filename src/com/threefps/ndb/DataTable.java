/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.errors.NotFoundException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Implementation of the Table interface
 * 
 * Version 1 header format (total 2305 bytes):
 * - Version: 1 byte
 * - Table name: 64 bytes, byte-zero padding to the right
 * - The next 64 * 35 bytes contains field informations:
 *   - Field name: 30 bytes
 *   - Data type: 1 byte
 *   - First data position: 4 bytes
 * 
 * @author sluu
 */
class DataTable implements Table {

    private static final byte CURRENT_VERSION = 1;
    private static final int NAME_MAX_LENGTH = 64;
    
    private byte version = 0;
    private String name = null;
    private FileChannel recFile = null; // Record file channel
    private FileChannel dataFile = null; // Data file channel
    
    // <editor-fold defaultstate="collapsed" desc="Getters and Setters">
    private void setRecFile(FileChannel recFile) {
        this.recFile = recFile;
    }
    
    private FileChannel getRecFile() {
        return recFile;
    }
    
    private void setDataFile(FileChannel dataFile) {
        this.dataFile = dataFile;
    }
    
    private FileChannel getDataFile() {
        return dataFile;
    }
    
    private void setName(String name) {
        this.name = name;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    private void setVersion(byte version) {
        this.version = version;
    }
    
    @Override
    public byte getVersion() {
        return version;
    }

    // </editor-fold>
    
    /**
     * Create or open a new 
     * @param name
     * @param dir
     * @param create
     * @return 
     */
    public static DataTable open(String name, String dir) throws IOException, DataException {
        name = name.toLowerCase().trim();        
        if (name.length() > DataTable.NAME_MAX_LENGTH) {
            name = name.substring(0, NAME_MAX_LENGTH);
        }
        
        Path recPath = FileSystems.getDefault().getPath(dir, name + ".rec");
        Path dataPath = FileSystems.getDefault().getPath(dir, name + ".dat");
        
        File recFile = recPath.toFile();
        boolean recFileExists = recFile.exists();
        if (!recFileExists) {
            recFile.createNewFile();
        }
        File dataFile = dataPath.toFile();        
        boolean dataFileExists = dataFile.exists();
        if (!dataFileExists) {
            dataFile.createNewFile();
        }
        
        DataTable table = new DataTable();
        table.setRecFile(FileChannel.open(recPath, StandardOpenOption.READ, StandardOpenOption.WRITE));
        table.setDataFile(FileChannel.open(dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE));
        
        if (dataFileExists && recFileExists) {
            table.loadHeader();
        } else {
            table.setName(name);
            table.setVersion(DataTable.CURRENT_VERSION);
            table.writeHeader();
        }
        return table;
    }
    
    /**
     * Load table header from file
     */
    private void loadHeader() throws DataException, IOException {        
        FileChannel f = getRecFile();
        int nread = 0;
        // read version
        ByteBuffer buff = ByteBuffer.allocate(1);
        if (f.read(buff) != 1) {
            throw new DataException("Cannot read header version");
        }
        setVersion(buff.get());
    }
    
    /**
     * Write table header to file
     */
    private void writeHeader() throws IOException {
        // Allocate bytes for the header
                
        byte[] nameBytes = getName().getBytes();        
        
        ByteBuffer buff = ByteBuffer.allocate(2305);
        buff.put(getVersion()).put(nameBytes);
        
        // write the padding 
        int padding = DataTable.NAME_MAX_LENGTH - nameBytes.length;
        while (padding > 0) {
            buff.put((byte)0);
            padding--;            
        }
        // fill in empty fields
        for (int i = 0; i < 2240; i++) {
            buff.put((byte)0);
        }
        
        FileChannel f = getRecFile();
        f.position(0);
        buff.flip();
        while (buff.hasRemaining()) {
            f.write(buff);
        }
        f.force(true);
    }
    
    /**
     * Close this table and files associated with it
     */
    public void close() throws IOException {
        if (getRecFile() != null && getRecFile().isOpen()) {
            getRecFile().close();
        }
        if (getDataFile() != null && getDataFile().isOpen()) {
            getDataFile().close();
        }
    }
    
    
    @Override
    public Record newRecord() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Record getRecord(int id) throws NotFoundException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
}
