package org.problem;

import it.unimi.dsi.fastutil.Hash;
import org.apache.commons.lang3.Range;
import org.apache.commons.math3.util.Pair;
import scala.Int;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CommonAges {
    /**
     * Returns the most common ages
     *
     * @param dataIterator          ages iterator
     * @param numberMostRepresented number of most represented ages
     *                              returns list of most common ages
     **/
    public static List<Integer> mostCommonAges(Iterator<Integer> dataIterators,
                                        int numberMostRepresented) {
        var map = new HashMap<Integer, Integer>();

        dataIterators.forEachRemaining(x-> {
            if (map.containsKey(x)) {
                map.put(x, map.get(x) + 1);
            } else {
                map.put(x, 1);
            }
        });
        var ageCounts = new LinkedList<Map.Entry<Integer, Integer>>(map.entrySet());

        return ageCounts.stream()
                .sorted( (b, a) -> a.getValue().compareTo(b.getValue()))
                .limit(numberMostRepresented)
                .map(x-> x.getKey())
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        var ages = List.of(30, 54, 67, 60, 51, 53, 54, 51, 54, 30, 51, 54).iterator();
        var result = mostCommonAges(ages, 2);
        System.out.println(Arrays.toString(result.toArray()));
    }
}