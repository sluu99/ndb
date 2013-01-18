/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.errors;

/**
 * Thrown when there's an error reading table header
 * @author sluu
 */
public class DataException extends Exception {
    public DataException(String message) {
        super(message);
    }
}
