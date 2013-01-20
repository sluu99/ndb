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
 * Value node
 *
 * @author sluu
 */
public class Value extends Node {

    private long recordPos = 0;
    private byte flag = 1;
    private long timestamp;
    private DataType type;
    private long prevVersionPos = 0;
    private long twinPos = 0;
    private long leftPos = 0;
    private long rightPos = 0;
    private byte[] raw = null; // raw data

    private Value() {
    }

    // <editor-fold desc="Getters & Setters">
    private void setRecordPos(long recordPos) {
        this.recordPos = recordPos;
    }

    private void setFlag(byte flag) {
        this.flag = flag;
    }

    private void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    private void setType(DataType type) {
        this.type = type;
    }

    private void setPrevVersionPos(long prevVersionPos) {
        this.prevVersionPos = prevVersionPos;
    }

    private void setTwinPos(long twinPos) {
        this.twinPos = twinPos;
    }

    private void setLeftPos(long leftPos) {
        this.leftPos = leftPos;
    }

    private void setRightPos(long rightPos) {
        this.rightPos = rightPos;
    }

    private void setRaw(byte[] raw) {
        this.raw = raw;
    }

    public long getRecordPos() {
        return recordPos;
    }

    public byte getFlag() {
        return flag;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DataType getType() {
        return type;
    }

    public long getPrevVersionPos() {
        return prevVersionPos;
    }

    public long getTwinPos() {
        return twinPos;
    }

    public long getLeftPos() {
        return leftPos;
    }

    public long getRightPos() {
        return rightPos;
    }

    public byte[] getRaw() {
        return raw;
    }
    // </editor-fold>

    /**
     * Write the value to file
     *
     * @param f
     * @throws IOException
     */
    private void write(DataFile f) throws IOException {
        byte[] data = getRaw();
        int size = POINTER_SIZE * 5 + 2 + TIMESTAMP_SIZE + data.length;
        byte[] buff = new byte[size];
        int offset = 0;
        arraycopy(B.fromLong(getRecordPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        buff[offset] = getFlag();
        offset += 1;
        arraycopy(B.fromLong(getTimestamp()), 0, buff, offset, TIMESTAMP_SIZE);
        offset += TIMESTAMP_SIZE;
        arraycopy(B.fromLong(getPrevVersionPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getTwinPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getLeftPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        arraycopy(B.fromLong(getRightPos()), 0, buff, offset, POINTER_SIZE);
        offset += POINTER_SIZE;
        buff[offset] = getType().toByte();
        offset += 1;
        arraycopy(data, 0, buff, offset, data.length);
        f.write(getPos(), buff, 0, buff.length);
    }

    /**
     * Create a new value and write it to file
     *
     * @param recordPos
     * @param prevVersionPos
     * @param type
     * @param data
     * @return
     */
    public static Value create(DataFile f, long recordPos, long prevVersionPos, DataType type, byte[] data) throws IOException {
        Value v = new Value();
        synchronized (f) {
            v.setPos(f.getChannel().size());
            v.setRecordPos(recordPos);
            v.setFlag((byte) 1);
            v.setTimestamp(System.currentTimeMillis() / 1000);
            v.setPrevVersionPos(prevVersionPos);
            v.setType(type);
            v.setRaw(data);
            v.write(f);
        }
        return v;
    }

    /**
     * Read a value from file
     * @param f
     * @param pos
     * @return
     * @throws DataException
     * @throws IOException 
     */
    public static Value read(DataFile f, long pos) throws DataException, IOException {
        if (pos == 0) {
            return null;
        }

        int size = POINTER_SIZE * 5 + 2 + TIMESTAMP_SIZE;
        byte[] buff = new byte[size];
        if (f.read(pos, buff, 0, size) != size) {
            throw new DataException("Cannot read value header");
        }

        Value v = new Value();
        int offset = 0;

        v.setRecordPos(B.toLong(buff, offset));
        offset += POINTER_SIZE;
        v.setFlag(buff[offset]);
        offset++;
        v.setTimestamp(B.toLong(buff, offset));
        offset += TIMESTAMP_SIZE;
        v.setPrevVersionPos(B.toLong(buff, offset));
        offset += POINTER_SIZE;
        v.setTwinPos(B.toLong(buff, offset));
        offset += POINTER_SIZE;
        v.setLeftPos(B.toLong(buff, offset));
        offset += POINTER_SIZE;
        v.setRightPos(B.toLong(buff, offset));
        offset += POINTER_SIZE;
        v.setType(DataType.fromByte(buff[offset]));
        offset++;

        // read the next few bytes for the data or string length
        DataType t = v.getType();
        size = t.size();
        buff = new byte[size];
        if (f.read(pos + offset, buff, 0, size) != size) {
            throw new DataException("Cannot read value data");
        }
        offset += size;

        // if the type is string, we have only read the string length
        if (t == DataType.STRING) {            
            byte len = buff[0];
            buff = new byte[len];
            if (f.read(pos + offset, buff, 0, len) != len)
                throw new DataException("Cannot read string value");
            
        } else if(t == DataType.BIG_STRING || t == DataType.BINARY) {
            int len = B.toInt(buff, 4);
            buff = new byte[len];
            if (f.read(pos + offset, buff, 0, len) != len)
                throw new DataException("Cannot read big string value or binary");
        }
        
        v.setRaw(buff);

        return v;
    }
}
