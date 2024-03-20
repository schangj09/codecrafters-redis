package org.baylight.redis.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OrderedArrayList<T extends Comparable<T>>  extends ArrayList<T> {
    int find(T val) {
        // could use binary search, but for now just indexOf
        return indexOf(val);
    }

    T last() {
        return size() > 0 ? get(size() - 1) : null;
    }

    public List<T> range(T startId, T endId) {
        // could use binary search, but for now just scan the array
        return stream().filter(id -> id.compareTo(startId) >= 0 && id.compareTo(endId) <= 0)
                .collect(Collectors.toList());
    }

}
