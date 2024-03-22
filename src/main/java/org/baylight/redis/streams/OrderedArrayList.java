package org.baylight.redis.streams;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class OrderedArrayList<T extends Comparable<T>> extends ArrayList<T> {

    // get the index of the next item - this is the left most item which is greater than val
    // returns 0 if val is smaller than all items and returns size() if val is
    // greater than all items
    int findNext(T val) {
        if (size() == 0 || val.compareTo(get(0)) < 0) {
            return 0;
        }
        if (val.compareTo(last()) >= 0) {
            return size();
        }
        return find(0, size(), i -> get(i).compareTo(val) > 0);
    }

    // get the left most index of the value that matches the condition
    static int find(int left, int right, Function<Integer, Boolean> matchCondition) {
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (matchCondition.apply(mid)) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    T last() {
        return size() > 0 ? get(size() - 1) : null;
    }

    public List<T> range(T startId, T endId) {
        int i = find(0, size(), m -> get(m).compareTo(startId) >= 0);
        int j = findNext(endId);
        return subList(i, j);
    }

}
