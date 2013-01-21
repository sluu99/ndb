/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.Const;
import static com.threefps.ndb.Const.*;
import com.threefps.ndb.DataType;
import com.threefps.ndb.errors.DataException;
import com.threefps.ndb.utils.B;
import java.io.IOException;
import static java.lang.System.arraycopy;
import java.util.ArrayList;
import java.util.Locale;

/**
 * A key node
 *
 * @author sluu
 */
public class Key extends Node {

    /**
     * Position of the record this key belongs to
     */
    private long recordPos = 0;
    /**
     * Position of the next key within the same record
     */
    private long nextPos = 0;
    /**
     * Position of the value node
     */
    private long valuePos = 0;
    /**
     * The key name
     */
    private String name = null;
    /**
     * The value object associated with this key (lazy loaded)
     */
    private ValueImpl value = null;

    /**
     * Create a new record and write to file.
     *
     * @param f Data file toe write to
     * @param recordPos Position of the record that this key belongs to
     * @param nextPos Position of the next key within the same record
     * @param name The Key name
     * @return The newly created Key object
     * @throws IOException IF an I/O error occurred
     * @throws DataException If the key data exceeds the underlying page size
     */
    public static Key create(DataFile f, long recordPos, long nextPos, String name) throws IOException, DataException {
        Key k = new Key();
        k.setRecordPos(recordPos);
        k.setNextPos(nextPos);
        k.setName(name);
        k.create(f);
        return k;
    }

    /**
     * Read a key from file
     *
     * @param f The file to read from
     * @param pos Position of the key in file
     * @return The key object
     * @throws IOException If an I/O error occurred
     */
    public static Key read(DataFile f, long pos) throws IOException {
        if (pos <= 0) {
            return null;
        }

        Key key = new Key();
        key.setPos(pos);

        // read the key over head
        int size = POINTER_SIZE * 3 + 1;
        byte[] buff = new byte[size];
        f.read(pos, buff, 0, size);
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
        f.read(pos + offset, buff, 0, size);
        key.setName(new String(buff, 0, size, Const.CHARSET));

        return key;
    }

    // <editor-fold desc="Getters and Setters">
    /**
     * Set the key value
     *
     * @param v The value object
     */
    private void setValue(ValueImpl v) {
        this.value = v;
    }

    /**
     * Get position of the record that this key belongs to
     *
     * @return The record position
     */
    public long getRecordPos() {
        return recordPos;
    }

    /**
     * Set position of the record that this key belongs to
     *
     * @param recordPos The record position
     */
    public void setRecordPos(long recordPos) {
        this.recordPos = recordPos;
    }

    /**
     * Get the position of the next key within the same record
     *
     * @return The position of that key
     */
    public long getNextPos() {
        return nextPos;
    }

    /**
     * Set the position of the next key within the same record
     *
     * @param nextPos
     */
    public void setNextPos(long nextPos) {
        this.nextPos = nextPos;
    }

    /**
     * Get the position of the value node associated with this key
     *
     * @return Value node position
     */
    public long getValuePos() {
        return valuePos;
    }

    /**
     * Set the position of the value node associated with this key. If the value
     * position is different from the current one, the value object will be set
     * to null until the next time it is lazily loaded.
     *
     * @param valuePos Value node key
     */
    public void setValuePos(long valuePos) {
        this.valuePos = valuePos;
        if (value != null && value.getPos() != valuePos) {
            value = null;
        }
    }

    /**
     * Get the key name
     *
     * @return Key name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the key name
     *
     * @param name The name
     */
    public void setName(String name) {
        this.name = name.toLowerCase(Locale.getDefault()).trim();
    }
    // </editor-fold>

    // <editor-fold desc="Writters">
    /**
     * Write the record position to file
     *
     * @param f The data file
     * @throws IOException If an I/O error occurred
     */
    public void writeRecordPos(DataFile f) throws IOException {
        f.write(getPos(), B.fromLong(getRecordPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the next key position to file
     *
     * @param f The data file
     * @throws IOException If an I/O error occurred
     */
    public void writeNextPos(DataFile f) throws IOException {
        f.write(getPos() + POINTER_SIZE, B.fromLong(getNextPos()), 0, POINTER_SIZE);
    }

    /**
     * Write the value position to file
     *
     * @param f The data file
     * @throws IOException If an I/O error occurred
     */
    public void writeValuePos(DataFile f) throws IOException {
        f.write(
                getPos() + POINTER_SIZE * 2,
                B.fromLong(getValuePos()), 0, POINTER_SIZE);
    }

    /**
     * Write the key name
     *
     * @param f The data file
     * @throws IOException If an I/O error occurred
     */
    public void writeName(DataFile f) throws IOException {
        byte[] buff = B.fromString(getName());
        long offset = getPos() + POINTER_SIZE * 3;
        f.write(offset, buff, 0, buff.length);
    }

    /**
     * Write the new key to file
     *
     * @param f The data file
     * @throws IOException If an I/O error occurred
     * @throws DataException If the key size is larger than the underlying page
     * size
     */
    public void create(DataFile f) throws IOException, DataException {
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
        setPos(f.append(buff, 0, buff.length));
    }

    /**
     * Write a new value for this key
     *
     * @param f The file to write to
     * @param type The data type
     * @param val raw value
     * @throws IOException IOException If an I/O error occurred
     * @throws DataException If the value node size is larger than the
     * underlying page size while creating the value
     */
    public void writeValue(DataFile f, DataType type, byte[] val) throws IOException, DataException {
        ValueImpl v = ValueImpl.create(f, getRecordPos(), getValuePos(), type, val);
        retireValue(f);
        setValuePos(v.getPos());
        writeValuePos(f);
        setValue(v);
    }
    // </editor-fold>

    /**
     * Retire an old value and by writing the flag to file
     * @param f The data file
     * @throws IOException IOException If an I/O error occurred
     */
    private void retireValue(DataFile f) throws IOException {
        long pos = getValuePos();
        if (pos != 0) {
            f.write(getValuePos() + POINTER_SIZE, B.fromByte((byte) 0), 0, 1);
        }
    }

    /**
     * Get the current value of this key
     *
     * @param f
     * @return
     * @throws DataException
     * @throws IOException
     */
    public ValueImpl getValue(DataFile f) throws DataException, IOException {
        if (value == null && getPos() != 0) {
            value = ValueImpl.read(f, getValuePos());
        }
        return value;
    }

    /**
     * Get the previous n values for this key
     *
     * @param f
     * @param n
     * @return
     * @throws DataException
     * @throws IOException
     */
    public ValueImpl[] getValues(DataFile f, int n) throws DataException, IOException {
        ValueImpl v = getValue(f);
        if (v == null) {
            return new ValueImpl[0];
        }
        ArrayList<ValueImpl> values = new ArrayList<>(n);
        do {
            values.add(v);
            v = v.previous(f);
        } while (v != null);

        ValueImpl[] arr = new ValueImpl[values.size()];
        values.toArray(arr);
        return arr;
    }
}
