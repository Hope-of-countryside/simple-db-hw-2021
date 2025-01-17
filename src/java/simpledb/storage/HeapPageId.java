/*
 * @Author: zyx 625762527@qq.com
 * @Date: 2022-07-25 14:42:02
 * @LastEditors: zyx 625762527@qq.com
 * @LastEditTime: 2022-09-25 17:00:12
 * @FilePath: /simple-db-hw-2021/src/java/simpledb/storage/HeapPageId.java
 * @Description:
 *
 * Copyright (c) 2022 by zyx 625762527@qq.com, All Rights Reserved.
 */
package simpledb.storage;

import java.util.Objects;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private int tableId;

    private int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        // int[] a = {1,2};
        // return Arrays.hashCode(a);
        return Objects.hash(tableId, pgNo);
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || o.getClass() != getClass())
            return false;
        HeapPageId heapPageId = (HeapPageId) o;
        return this.tableId == heapPageId.tableId && this.pgNo == heapPageId.pgNo;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk. Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

    public String toString(){
        return "tableId: " + this.tableId + " page number: " + this.getPageNumber() + "\t";
    }

}
