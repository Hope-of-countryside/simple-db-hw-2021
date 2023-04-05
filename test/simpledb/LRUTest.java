package simpledb;

import org.junit.Test;
import simpledb.common.DbException;
import simpledb.util.LRU;
import org.junit.Assert;

import java.util.Iterator;


public class LRUTest {

    @Test
    public void testLRUBasicFunction() throws DbException {
        int capacity = 10;
        int overflow = 100;
        LRU<Integer, Integer> lru = new LRU<>(capacity);
        for (int i = 0; i < capacity + overflow; i++) {
            lru.put(i, i, null);
        }
        Assert.assertEquals(lru.size(), capacity);
        Iterator<Integer> iterator = lru.iterator2();
        for (int i = 0; i < capacity; i++) {
            int current = iterator.next();
            Assert.assertEquals(current, overflow + i);
        }
    }
}
