/*
 * @Author: zyx 625762527@qq.com
 * @Date: 2022-07-25 14:42:02
 * @LastEditors: zyx 625762527@qq.com
 * @LastEditTime: 2022-08-06 14:51:01
 * @FilePath: /simple-db-hw-2021/src/java/simpledb/storage/RecordId.java
 * @Description:
 *
 * Copyright (c) 2022 by zyx 625762527@qq.com, All Rights Reserved.
 */
package simpledb.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pageId;
    private int tupleNumber;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid
     *                the pageid of the page on which the tuple resides
     * @param tupleno
     *                the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pageId = pid;
        this.tupleNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o == this)
            return true;
        if (o == null || o.getClass() != getClass())
            return false;
        RecordId recordId = (RecordId) o;
        return recordId.pageId.equals(this.pageId) && recordId.tupleNumber == this.tupleNumber;
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        return Objects.hash(tupleNumber, pageId);
        // throw new UnsupportedOperationException("implement this");

    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("pageId: ").append(this.pageId.toString()).append(" ");
        return sb.toString();
    }

}
