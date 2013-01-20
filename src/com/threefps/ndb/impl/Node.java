/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.impl;

/**
 *
 * @author sluu
 */
abstract class Node {
    
    private long pos = 0;
    
    /**
     * Get position of the node in the file
     * @return 
     */
    public long getPos() {
        return pos;
    }
    
    /**
     * Set position of the node
     * @param pos 
     */
    public void setPos(long pos) {
        this.pos = pos;
    }
    
}
