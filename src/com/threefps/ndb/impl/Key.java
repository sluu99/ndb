/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import static com.threefps.ndb.Const.*;
import com.threefps.ndb.utils.B;
import com.threefps.ndb.utils.IO;
import java.io.IOException;
import static java.lang.System.arraycopy;
import java.nio.channels.FileChannel;

/**
 *
 * @author sluu
 */
public class Key extends Node{
    
    private long recordPos = 0; 
    private long nextPos = 0;
    private long valuePos = 0;
    private byte nameSize = 0;
    private String name = null;

    /**
     * Create a new record and write to file
     * @param f
     * @param recordPos
     * @param name
     * @return 
     */
    public static Key create(FileChannel f, long recordPos, long nextPos, String name) throws IOException {
        Key k = new Key();
        synchronized(f) {
            k.setPos(f.size());
            k.setRecordPos(recordPos);
            k.setNextPos(nextPos);
            k.setName(name);
            k.write(f);
        }
        return k;
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
    }

    public byte getNameSize() {
        return nameSize;
    }

    public void setNameSize(byte nameSize) {
        this.nameSize = nameSize;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name.trim().toLowerCase();
    }  
    // </editor-fold>
    
    public void writeRecordPos(FileChannel f) throws IOException {
        IO.write(f, getPos(), B.fromLong(getRecordPos()), 0, POINTER_SIZE);
    }
    
    /**
     * Write the next key position to file
     * @throws IOException 
     */
    public void writeNextPos(FileChannel f) throws IOException {
        IO.write(f, getPos() + POINTER_SIZE, B.fromLong(getNextPos()), 0, POINTER_SIZE);
    }
    
    /**
     * Write the value position to file
     * @throws IOException 
     */
    public void writeValuePos(FileChannel f) throws IOException {
        IO.write(
                f, getPos() + POINTER_SIZE * 2,
                B.fromLong(getValuePos()), 0, POINTER_SIZE);
    }
    
    /**
     * Write the key name
     * @throws IOException 
     */
    public void writeName(FileChannel f) throws IOException {
        byte[] buff = B.fromString(getName());
        long offset = getPos() + POINTER_SIZE * 3;        
        IO.write(f, offset, buff, 0, buff.length);
    }
    
    /**
     * Write the key to file
     */
    public void write(FileChannel f) throws IOException {
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
        IO.write(f, getPos(), buff, 0, buff.length);
    }
    
}
