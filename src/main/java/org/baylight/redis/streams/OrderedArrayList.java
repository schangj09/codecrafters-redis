package org.baylight.redis.streams;

import java.util.ArrayList;

public class OrderedArrayList<T> extends ArrayList<T> {
    int find(T val) {
        // could use binary search, but for now just indexOf
        return indexOf(val);
    }

    T last() {
        return size() > 0 ? get(size() - 1) : null;
    }

}