/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.errors.LimitException;
import com.threefps.ndb.errors.NotFoundException;
import com.threefps.ndb.utils.IO;
import com.threefps.ndb.utils.Str;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of the Table interface
 * 
 * Version 1 header format (total 2305 bytes):
 * - Version: 1 byte
 * - Table name: 64 bytes, byte-zero padding to the right
 * - The next 64 * 35 bytes contains field informations:
 *   - Data type: 1 byte
 *   - Field name: 30 bytes
 *   - First data position: pointer
 * 
 * @author sluu
 */
class DataTable implements Table {

    private static final byte CURRENT_VERSION = 1;
    
    private static final byte MAX_FIELDS = 64;
    
    private static final int VERSION_SIZE = 1;
    private static final int TABLE_NAME_SIZE = 64;
    private static final int DATA_TYPE_SIZE = 1;
    private static final int FIELD_NAME_SIZE = 30;
    private static final int POINTER_SIZE = 8; // Pointer to a file position
    private static final int FIELD_INFO_SIZE = DATA_TYPE_SIZE + FIELD_NAME_SIZE + POINTER_SIZE;
    private static final int HEADER_SIZE = VERSION_SIZE + TABLE_NAME_SIZE + (FIELD_INFO_SIZE * MAX_FIELDS);
    
    private byte version = 0;
    private String name = null;
    private FileChannel recFile = null; // Record file channel
    private FileChannel dataFile = null; // Data file channel
    private FieldInfo[] fields = new FieldInfo[MAX_FIELDS];
    
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
    
    // <editor-fold desc="Read header from file">
    
    /**
     * Load table header from file
     */
    private void readHeader() throws DataException, IOException {
        readVersion();
        readName();
        for (byte i = 0; i < MAX_FIELDS; i++) readField(i);
    }
    
    /**
     * Read the table version from file
     * @throws DataException
     * @throws IOException 
     */
    private void readVersion() throws DataException, IOException {
        FileChannel f = getRecFile();
        f.position(0);
        ByteBuffer buff = IO.read(f, VERSION_SIZE);        
        if (buff.position() != VERSION_SIZE)
            throw new DataException("Cannot read header version");
        buff.rewind();
        setVersion(buff.get());
    }
    
    /**
     * Read the table name from file
     * @throws DataException
     * @throws IOException 
     */
    private void readName() throws DataException, IOException {        
        FileChannel f = getRecFile();
        f.position(1);
        ByteBuffer buff = IO.read(f, TABLE_NAME_SIZE);        
        if (buff.position() != TABLE_NAME_SIZE)
            throw new DataException("Cannot read table name");
        setName(Str.fromCString(buff.array()));
    }
    
    /**
     * Read the Nth field information
     * @param n Zero-based field number
     */
    private void readField(byte n) throws IOException, DataException {
        FileChannel f = getRecFile();
        f.position(VERSION_SIZE + TABLE_NAME_SIZE + (n * FIELD_INFO_SIZE));
        ByteBuffer buff = IO.read(f, FIELD_INFO_SIZE);
        if (buff.position() != FIELD_INFO_SIZE)
            throw new DataException("Fail to read field info for #" + n);
        buff.rewind();
        byte[] b = buff.array();
        if (DataType.fromByte(b[0]) == DataType.NONE)
            fields[n] = null;
        else {
            fields[n] = new FieldInfo(n,
                    Str.fromCString(b, 1, FIELD_NAME_SIZE), 
                    DataType.fromByte(b[0]), 
                    buff.getLong(FIELD_NAME_SIZE + DATA_TYPE_SIZE));
        }
    }
    // </editor-fold>
    
    // <editor-fold desc="Write header to file">
    
    /**
     * Write table header to file
     */
    private void writeHeader() throws IOException {
        writeVersion();
        writeName();
        for (int i = 0; i < MAX_FIELDS; i++) writeField(i);
        getRecFile().force(true);
    }
    
    /**
     * Write version to header
     * @throws IOException 
     */
    private void writeVersion() throws IOException {
        FileChannel f = getRecFile();
        f.position(0);
        ByteBuffer buff = ByteBuffer.allocate(1);
        buff.put(getVersion());
        buff.flip();
        IO.write(f, buff, false);
    }
    
    /**
     * Write table name to header
     * @throws IOException 
     */
    private void writeName() throws IOException {
        FileChannel f = getRecFile();
        f.position(VERSION_SIZE);
        ByteBuffer buff = ByteBuffer.allocate(TABLE_NAME_SIZE);
        buff.put(Str.toBuffer(getName(), TABLE_NAME_SIZE), 0, TABLE_NAME_SIZE);
        buff.flip();
        IO.write(f, buff, false);
    }
    
    /**
     * Write the Nth field information
     * @param n Zero-based field number
     */
    private void writeField(int n) throws IOException {
        FileChannel f = getRecFile();
        f.position(VERSION_SIZE + TABLE_NAME_SIZE + (n * FIELD_INFO_SIZE));
        
        if (fields[n] == null) 
            IO.writeZeros(f, FIELD_INFO_SIZE, true);
        else {
            ByteBuffer buff = ByteBuffer.allocate(FIELD_INFO_SIZE);
            buff.put(fields[n].getType().toByte());
            buff.put(Str.toBuffer(fields[n].getName(), 30));
            buff.putLong(fields[n].getFDP());
            buff.flip();
            IO.write(f, buff, false);
        }
        
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
        if (name.length() > FIELD_NAME_SIZE) {
            name = name.substring(0, FIELD_NAME_SIZE);
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
        
        table.setName(name);
        if (dataFileExists && recFileExists)
            table.readHeader();
        else {            
            table.setVersion(DataTable.CURRENT_VERSION);
            table.writeHeader();
        }
        return table;
    }
    
    /**
     * Close this table and files associated with it
     */
    public void close() throws IOException {
        if (getRecFile() != null && getRecFile().isOpen()) {
            getRecFile().force(true);
            getRecFile().close();
        }
        if (getDataFile() != null && getDataFile().isOpen()) {
            getDataFile().force(true);
            getDataFile().close();
        }
    }
    
    
    @Override
    public Record newRecord() throws IOException  {
        
        FileChannel f = getRecFile();
        long pos = f.size();
        f.position(pos);
        IO.writeZeros(f, MAX_FIELDS * POINTER_SIZE, true);
        DataRecord r = new DataRecord();
        r.setId((pos - HEADER_SIZE) / (MAX_FIELDS * POINTER_SIZE) + 1);
        return r;
    }

    @Override
    public Record getRecord(int id) throws NotFoundException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public FieldInfo getField() throws LimitException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
}
