package com.latticeengines.common.exposed.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class FileBackedOrderedListUnitTestNG {

    @AfterMethod(groups = "unit")
    public void afterMethod() {
        LogManager.getLogger(FileBackedOrderedList.class).setLevel(Level.INFO);
    }

    @Test(groups = "unit")
    public void test() {
        // LogManager.getLogger(FileBackedOrderedList.class).setLevel(Level.DEBUG);
        FileBackedOrderedList<Integer> list = new FileBackedOrderedList<>(100, s ->
                "".equals(s) ? null : Integer.valueOf(s)
        );
        Random random = new Random(System.currentTimeMillis());
        for (Integer n: data()) {
            list.add(n);
            if (random.nextInt(100) > 70) {
                list.add(null);
            }
        }
        int max = Integer.MIN_VALUE;
        for (Integer n: list) {
            if (n != null) {
                Assert.assertTrue(n >= max);
                max = Math.max(max, n);
            }
        }
    }

    private Collection<Integer> data() {
        List<Integer> ints = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            ints.add(i);
        }
        Collections.shuffle(ints);
        return ints;
    }
}
