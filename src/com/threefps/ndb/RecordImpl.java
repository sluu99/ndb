/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb;

import static com.threefps.ndb.Const.*;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
import com.threefps.ndb.utils.IO;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 *
 * @author sluu
 */
class RecordImpl extends Node implements Record {

    private long creationTime = 0;
    private long updateTime = 0;
    private FileChannel file = null;
    private long prevRecordPos = 0;
    private long keyPos = 0;

    // <editor-fold desc="Getters and Setters">
    @Override
    public long getId() {
        return getPos();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getUpdateTime() {
        return updateTime;
    }

    public void setCreationTime(long time) {
        creationTime = time;
    }

    public void setUpdateTime(long time) {
        updateTime = time;
    }

    public FileChannel getFile() {
        return file;
    }

    public void setFile(FileChannel file) {
        this.file = file;
    }

    public long getPrevRecordPos() {
        return prevRecordPos;
    }

    public void setPrevRecordPos(long pos) {
        prevRecordPos = pos;
    }

    public long getKeyPos() {
        return keyPos;
    }

    public void setKeyPos(long keyPos) {
        this.keyPos = keyPos;
    }
    // </editor-fold>

    /**
     * Check the position before writing
     *
     * @throws DataException
     */
    private void checkPos() throws DataException {
        if (getPos() <= 0) {
            throw new DataException("Cannot write record at position zero");
        }
    }

    /**
     * Write the creation timestamp to file
     *
     * @throws IOException
     * @throws DataException when record has position zero
     */
    public void writeCreationTime() throws IOException, DataException {
        checkPos();
        IO.write(
                getFile(), getPos(),
                B.fromLong(getCreationTime()), 0, TIMESTAMP_SIZE);
    }

    /**
     * Write the update timestamp to file
     *
     * @throws DataException
     * @throws IOException
     */
    public void writeUpdateTime() throws DataException, IOException {
        checkPos();
        IO.write(
                getFile(), getPos() + TIMESTAMP_SIZE,
                B.fromLong(getUpdateTime()), 0, TIMESTAMP_SIZE);
    }

    /**
     * Write previous record
     */
    public void writePrevRecordPos() throws DataException, IOException {
        checkPos();
        IO.write(
                getFile(), getPos() + TIMESTAMP_SIZE + TIMESTAMP_SIZE,
                B.fromLong(getPrevRecordPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the key position
     *
     * @throws DataException
     * @throws IOException
     */
    public void writeKeyPos() throws DataException, IOException {
        checkPos();
        IO.write(
                getFile(), getPos() + TIMESTAMP_SIZE + TIMESTAMP_SIZE + POINTER_SIZE,
                B.fromLong(getKeyPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the record to file
     *
     * @throws IOException
     * @throws DataException
     */
    public void write() throws IOException, DataException {
        writeCreationTime();
        writeUpdateTime();
        writePrevRecordPos();
        writeKeyPos();
    }

    /**
     * Read the record from file
     *
     * @throws IOException
     * @throws DataException
     */
    public void read() throws IOException, DataException {
        int size = TIMESTAMP_SIZE + TIMESTAMP_SIZE + POINTER_SIZE + POINTER_SIZE;
        byte[] b = new byte[size];
        if (IO.read(getFile(), getPos(), b, 0, size) != size) {
            throw new DataException("Cannot read record from file");
        }
        int offset = 0;
        setCreationTime(B.toLong(b, offset));
        offset += TIMESTAMP_SIZE;
        setUpdateTime(B.toLong(b, offset));
        offset += TIMESTAMP_SIZE;
        setPrevRecordPos(B.toLong(b, offset));
        offset += POINTER_SIZE;
        setKeyPos(B.toLong(b, offset));
    }

    /**
     * Create a new record and write it to file. The new record will have its
     * position and creation time populated.
     *
     * @param f
     * @param prevRecordPos Position of the previous record in the same table
     * @return
     */
    public static RecordImpl create(FileChannel f, long prevRecordPos) throws IOException, DataException {
        RecordImpl rec = new RecordImpl();
        rec.setFile(f);
        rec.setPos(f.size());
        rec.setPrevRecordPos(prevRecordPos);
        rec.setCreationTime(System.currentTimeMillis() / 1000);
        rec.write();
        return rec;
    }

    @Override
    public RecordImpl getPrevRecord() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
