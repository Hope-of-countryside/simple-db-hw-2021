package simpledb.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Least Recently Used Algorithm
 */
public class LRU<K, V> {
    // new element is inserted in tail
    // remove old element in head
    private final ArrayList<K> list;
    private final HashMap<K, V> map;
    private final int capacity;

    public LRU(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("invalid capacity");
        }
        list = new ArrayList<>();
        map = new HashMap<>();
        this.capacity = capacity;
    }

    public int size() {
        return list.size();
    }

    public void put(K k, V v) {
        if (map.containsKey(k)) {
            // update K V
            list.remove(k);
        } else if (size() >= capacity) {
            // remove Least Recently Used K V
            evict();
        }
        list.add(k);
        map.put(k, v);

    }

    public V evict() {
        if (size() == 0) {
            throw new IllegalStateException("size == 0, invalid evict");
        }
        K removed = list.remove(0);
        return map.remove(removed);
    }

    public V nextEvictElement(){
        return map.get(list.get(0));
    }

    public V remove(K k) {
        list.remove(k);
        return map.remove(k);
    }

    public V get(K k) {
        return map.get(k);
    }

    public Iterator<Map.Entry<K, V>> iterator1() {
        return map.entrySet().iterator();
    }

    public Iterator<K> iterator2() {
        return list.iterator();
    }

    public Iterator<V> iterator3() {
        return map.values().iterator();
    }

    public Boolean containsKey(K k) {
        return map.containsKey(k);
    }

}
