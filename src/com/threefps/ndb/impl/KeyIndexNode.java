/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

import com.threefps.ndb.impl.Node;

/**
 * Represents a key-index node
 * @author sluu
 */
public class KeyIndexNode extends Node {
    
    private String name = null;
    private long firstValuePos = 0;
    private KeyIndexNode left = null;
    private KeyIndexNode right = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null? null : name.trim().toLowerCase();
    }

    public long getFirstValuePos() {
        return firstValuePos;
    }

    public void setFirstValuePos(long firstValuePos) {
        this.firstValuePos = firstValuePos;
    }

    public KeyIndexNode getLeft() {
        return left;
    }

    public void setLeft(KeyIndexNode left) {
        this.left = left;
    }

    public KeyIndexNode getRight() {
        return right;
    }

    public void setRight(KeyIndexNode right) {
        this.right = right;
    }
    
    
}
