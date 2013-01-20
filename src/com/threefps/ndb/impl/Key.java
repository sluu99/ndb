/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.DataType;
import static com.threefps.ndb.Const.*;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
import com.threefps.ndb.utils.DataFile;
import java.io.IOException;
import static java.lang.System.arraycopy;

/**
 *
 * @author sluu
 */
public class Key extends Node {

    private long recordPos = 0;
    private long nextPos = 0;
    private long valuePos = 0;
    private String name = null;
    private Value value = null;

    /**
     * Create a new record and write to file
     *
     * @param f
     * @param recordPos
     * @param name
     * @return
     */
    public static Key create(DataFile f, long recordPos, long nextPos, String name) throws IOException {
        Key k = new Key();
        synchronized (f) {
            k.setPos(f.getChannel().size());
            k.setRecordPos(recordPos);
            k.setNextPos(nextPos);
            k.setName(name);
            k.write(f);
        }
        return k;
    }

    /**
     * Read a key from file
     *
     * @param f
     * @param pos
     * @return
     * @throws IOException
     * @throws DataException
     */
    public static Key read(DataFile f, long pos) throws IOException, DataException {
        if (pos <= 0) {
            return null;
        }

        Key key = new Key();
        key.setPos(pos);

        // read the key over head
        int size = POINTER_SIZE * 3 + 1;
        byte[] buff = new byte[size];
        if (f.read(pos, buff, 0, size) != size) {
            throw new DataException("Error reading record key");
        }
        int offset = 0;
        key.setRecordPos(B.toLong(buff, offset));
        offset += POINTER_SIZE;
        key.setNextPos(B.toLong(buff, offset));
        offset += POINTER_SIZE;
        key.setValuePos(B.toLong(buff, offset));
        offset += POINTER_SIZE;

        // read key name
        size = buff[offset];
        offset++;
        buff = new byte[size];
        if (f.read(pos + offset, buff, 0, size) != size) {
            throw new DataException("Error reading key name");
        }
        key.setName(new String(buff, 0, size));

        return key;
    }

    // <editor-fold desc="Getters and Setters">
    public long getRecordPos() {
        return recordPos;
    }

    public void setRecordPos(long recordPos) {
        this.recordPos = recordPos;
    }

    public long getNextPos() {
        return nextPos;
    }

    public void setNextPos(long nextPos) {
        this.nextPos = nextPos;
    }

    public long getValuePos() {
        return valuePos;
    }

    public void setValuePos(long valuePos) {
        this.valuePos = valuePos;
        if (value != null && value.getPos() != valuePos) {
            value = null;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name.trim().toLowerCase();
    }
    // </editor-fold>

    // <editor-fold desc="Writters">
    public void writeRecordPos(DataFile f) throws IOException {
        f.write(getPos(), B.fromLong(getRecordPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the next key position to file
     *
     * @throws IOException
     */
    public void writeNextPos(DataFile f) throws IOException {
        f.write(getPos() + POINTER_SIZE, B.fromLong(getNextPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the value position to file
     *
     * @throws IOException
     */
    public void writeValuePos(DataFile f) throws IOException {
        f.write(
                getPos() + POINTER_SIZE * 2,
                B.fromLong(getValuePos()), 0, POINTER_SIZE);
    }

    /**
     * Write the key name
     *
     * @throws IOException
     */
    public void writeName(DataFile f) throws IOException {
        byte[] buff = B.fromString(getName());
        long offset = getPos() + POINTER_SIZE * 3;
        f.write(offset, buff, 0, buff.length);
    }

    /**
     * Write the key to file
     */
    public void write(DataFile f) throws IOException {
        byte[] nameBuff = B.fromString(getName());
        byte[] buff = new byte[POINTER_SIZE * 3 + nameBuff.length];
        int offset = 0;
        arraycopy(B.fromLong(getRecordPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getNextPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getValuePos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(nameBuff, 0, buff, offset, nameBuff.length);
        f.write(getPos(), buff, 0, buff.length);
    }

    /**
     * Write a new value for this key
     *
     * @param f
     * @param type
     * @param val
     * @throws IOException
     */
    public void writeValue(DataFile f, DataType type, byte[] val) throws IOException {
        Value v = Value.create(f, getRecordPos(), getValuePos(), type, val);
        retireValue(f);
        setValuePos(v.getPos());
        writeValuePos(f);
    }
    // </editor-fold>

    /**
     * Retire an old value and by writing the flag to file
     */
    private void retireValue(DataFile f) throws IOException {
        long pos = getValuePos();
        f.write(getValuePos() + POINTER_SIZE, B.fromByte((byte) 0), 0, 1);
    }

    /**
     * Get the current value of this key
     *
     * @param f
     * @return
     * @throws DataException
     * @throws IOException
     */
    public Value getValue(DataFile f) throws DataException, IOException {
        if (value == null && getPos() != 0) {
            value = Value.read(f, getValuePos());
        }
        return value;
    }
}
