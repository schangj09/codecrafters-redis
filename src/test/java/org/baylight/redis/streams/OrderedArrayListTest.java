package org.baylight.redis.streams;

import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.Test;

public class OrderedArrayListTest implements WithAssertions {
    @Test
    public void testFindNext() {
        OrderedArrayList<Integer> list = new OrderedArrayList<>();
        // fill list with sequence of numbers
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        for (int i = 0; i < 10; i++) {
            assertThat(list.findNext(i)).isEqualTo(i + 1);
        }
    }

    @Test
    public void testRange() {
        OrderedArrayList<Integer> list = new OrderedArrayList<>();
        // add 6 odd numbers to sequence of numbers (1 - 11)
        for (int i = 0; i <= 10; i += 2) {
            list.add(i + 1);
        }

        assertThat(list.range(0, 4))
                .as(String.format("%d, %d", 0, 4))
                .isEqualTo(list.subList(0, 2));
        assertThat(list.range(1, 8))
                .as(String.format("%d, %d", 1, 8))
                .isEqualTo(list.subList(0, 4));
        assertThat(list.range(2, 11))
                .as(String.format("%d, %d", 2, 11))
                .isEqualTo(list.subList(1, 6));
        assertThat(list.range(5, 100))
                .as(String.format("%d, %d", 5, 100))
                .isEqualTo(list.subList(2, 6));
    }
}
