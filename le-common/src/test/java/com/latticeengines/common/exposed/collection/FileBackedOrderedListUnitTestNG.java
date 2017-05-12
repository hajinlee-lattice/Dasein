package com.latticeengines.common.exposed.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class FileBackedOrderedListUnitTestNG {

    @AfterMethod
    public void afterMethod() {
        LogManager.getLogger(FileBackedOrderedList.class).setLevel(Level.INFO);
    }

    @Test
    public void test() {
        LogManager.getLogger(FileBackedOrderedList.class).setLevel(Level.DEBUG);
        FileBackedOrderedList<Integer> list = new FileBackedOrderedList<>(20, Integer::valueOf);
        for (Integer n: data()) {
            list.add(n);
        }
        int max = Integer.MIN_VALUE;
        for (Integer n: list) {
            Assert.assertTrue(n > max);
            max = Math.max(max, n);
        }
    }

    private Collection<Integer> data() {
        List<Integer> ints = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ints.add(i);
        }
        Collections.shuffle(ints);
        return ints;
    }
}
