/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.threefps.ndb.errors;

/**
 *
 * @author sluu
 */
public class LimitException extends Error {
    public LimitException(String msg) {
        super(msg);
    }
}
