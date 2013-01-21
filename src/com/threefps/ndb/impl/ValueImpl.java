/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.Const;
import static com.threefps.ndb.Const.*;
import com.threefps.ndb.DataType;
import com.threefps.ndb.Value;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
import java.io.IOException;
import static java.lang.System.arraycopy;

/**
 * Value node
 *
 * @author sluu
 */
public class ValueImpl extends Node implements Value {

    // <editor-fold desc="Static methods">
    /**
     * Create a new value and write it to file
     *
     * @param recordPos Position of the record
     * @param prevVersionPos Position of the previous version of this data
     * @param type The data type
     * @param data The raw data
     *
     * @return The newly created value
     * @throws IOException If an IO error occurred
     * @throws DataException If the value is larger than the underlying page
     * size
     */
    public static ValueImpl create(DataFile f, long recordPos, long prevVersionPos, DataType type, byte[] data) throws IOException, DataException {
        ValueImpl v = new ValueImpl();
        synchronized (f) {
            v.setRecordPos(recordPos);
            v.setFlag((byte) 1);
            v.setTimestamp(System.currentTimeMillis() / 1000);
            v.setPrevVersionPos(prevVersionPos);
            v.setType(type);
            v.setRaw(data);
            v.create(f);
        }
        return v;
    }

    /**
     * Read a value from file
     *
     * @param f The data file
     * @param pos Position of the value node
     * @return The value node
     * @throws IOException If an I/O error occurred
     */
    public static ValueImpl read(DataFile f, long pos) throws IOException {
        if (pos == 0) {
            return null;
        }

        int size = POINTER_SIZE * 5 + 2 + TIMESTAMP_SIZE;
        byte[] buff = new byte[size];
        f.read(pos, buff, 0, size);

        ValueImpl v = new ValueImpl();
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
        f.read(pos + offset, buff, 0, size);
        offset += size;

        // if the type is string, we have only read the string length
        if (t == DataType.STRING) {
            byte len = buff[0];
            buff = new byte[len];
            f.read(pos + offset, buff, 0, len);

        } else if (t == DataType.BIG_STRING || t == DataType.BINARY) {
            int len = B.toInt(buff, 4);
            buff = new byte[len];
            f.read(pos + offset, buff, 0, len);
        }

        v.setRaw(buff);

        return v;
    }
    // </editor-fold>
    /**
     * Record position
     */
    private long recordPos = 0;
    /**
     * Flags
     */
    private byte flag = 1;
    /**
     * Timestamp
     */
    private long timestamp;
    /**
     * Data type
     */
    private DataType type;
    /**
     * Position of the previous version
     */
    private long prevVersionPos = 0;
    /**
     * Position of the node that has the same data
     */
    private long twinPos = 0;
    /**
     * Position of the left child node
     */
    private long leftPos = 0;
    /**
     * Position of the right child node
     */
    private long rightPos = 0;
    /**
     * Raw data
     */
    private byte[] raw = null;

    private ValueImpl() {
    }

    // <editor-fold desc="Getters & Setters">
    /**
     * Set record position
     *
     * @param recordPos Record position
     */
    private void setRecordPos(long recordPos) {
        this.recordPos = recordPos;
    }

    /**
     * Set flag
     *
     * @param flag Flag
     */
    private void setFlag(byte flag) {
        this.flag = flag;
    }

    /**
     * Set timestamp
     *
     * @param timestamp Timestamp
     */
    private void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Set data type
     *
     * @param type Data type
     */
    private void setType(DataType type) {
        this.type = type;
    }

    /**
     * Set position of the previous version
     *
     * @param prevVersionPos Position
     */
    private void setPrevVersionPos(long prevVersionPos) {
        this.prevVersionPos = prevVersionPos;
    }

    /**
     * Set position of the twin node (node with the same data)
     *
     * @param twinPos Position
     */
    private void setTwinPos(long twinPos) {
        this.twinPos = twinPos;
    }

    /**
     * Set position of the left child node
     *
     * @param twinPos Position
     */
    private void setLeftPos(long leftPos) {
        this.leftPos = leftPos;
    }

    /**
     * Set position of the right child node
     *
     * @param twinPos Position
     */
    private void setRightPos(long rightPos) {
        this.rightPos = rightPos;
    }

    /**
     * Set raw data
     *
     * @param raw raw data
     */
    private void setRaw(byte[] raw) {
        this.raw = raw;
    }

    /**
     * Get record position
     *
     * @return Position
     */
    public long getRecordPos() {
        return recordPos;
    }

    /**
     * Get flag
     *
     * @return Flag
     */
    public byte getFlag() {
        return flag;
    }

    /**
     * Get timestamp
     *
     * @return Timestamp
     */
    @Override
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Get data type
     *
     * @return Data type
     */
    @Override
    public DataType getType() {
        return type;
    }

    /**
     * Get position of the previous version
     *
     * @return Position
     */
    public long getPrevVersionPos() {
        return prevVersionPos;
    }

    /**
     * Get position of the twin node
     *
     * @return Position
     */
    public long getTwinPos() {
        return twinPos;
    }

    /**
     * Get position of the left child node
     *
     * @return Position
     */
    public long getLeftPos() {
        return leftPos;
    }

    /**
     * Get position of the right child node
     *
     * @return Position
     */
    public long getRightPos() {
        return rightPos;
    }
    // </editor-fold>

    /**
     * Write the value to file
     *
     * @param f The data file
     * @throws IOException If an IO error occurred
     * @throws DataException If the value size is larger than the page size
     */
    private void create(DataFile f) throws IOException, DataException {
        byte[] data = raw();
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
        setPos(f.append(buff, 0, buff.length));

        // strip the length bytes off string types
        if (type == DataType.STRING || type == DataType.BIG_STRING) {
            byte[] newRaw = new byte[raw.length - type.size()];
            arraycopy(raw, type.size(), newRaw, 0, newRaw.length);
            setRaw(newRaw);
        }
    }

    /**
     * Get the previous version of this value
     *
     * @param f The data file
     * @throws IOException If an IO error occurred
     * @return The previous version or null if there's none
     */
    public ValueImpl previous(DataFile f) throws IOException {
        if (getPrevVersionPos() == 0) {
            return null;
        } else {
            return ValueImpl.read(f, getPrevVersionPos());
        }
    }

    /**
     * Make sure that the current value is of certain types or throw a
     * DataException
     *
     * @param ts The expected types
     * @throws DataException If the current type is not one of the expected
     * types
     */
    private void assertType(DataType... ts) throws DataException {
        DataType t = getType();
        for (DataType dt : ts) {
            if (dt == t) {
                return;
            }
        }
        throw new DataException("This value is of type " + t + ", not " + ts[0]);
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public byte asByte() throws DataException {
        assertType(DataType.BYTE);
        return raw[0];
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public short asShort() throws DataException {
        assertType(DataType.SHORT);
        return B.toShort(raw, 0);
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public int asInt() throws DataException {
        assertType(DataType.INT);
        return B.toInt(raw, 0);
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public long asLong() throws DataException {
        assertType(DataType.LONG);
        return B.toLong(raw, 0);
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public float asFloat() throws DataException {
        assertType(DataType.FLOAT);
        return B.toFloat(raw, 0);
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public double asDouble() throws DataException {
        assertType(DataType.DOUBLE);
        return B.toDouble(raw, 0);
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public boolean asBool() throws DataException {
        assertType(DataType.BOOL);
        return raw[0] == 1;
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public String asString() throws DataException {
        assertType(DataType.STRING, DataType.BIG_STRING);
        return new String(raw, 0, raw.length, Const.CHARSET);
    }

    /**
     * Get the value as a certain type
     *
     * @return The data in that type
     * @throws DataException If the current value is not of that type
     */
    @Override
    public byte[] asBin() throws DataException {
        assertType(DataType.BINARY);
        return raw.clone();
    }

    /**
     * Get the raw data
     *
     * @return Raw data
     */
    @Override
    public byte[] raw() {
        return raw.clone();
    }
}
