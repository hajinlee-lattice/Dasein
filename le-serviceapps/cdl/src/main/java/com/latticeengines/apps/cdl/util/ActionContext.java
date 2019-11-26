package com.latticeengines.apps.cdl.util;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.latticeengines.domain.exposed.pls.Action;

public class ActionContext {
    private static Logger log = LoggerFactory.getLogger(ActionContext.class);

    private static ThreadLocal<Action> action = new ThreadLocal<Action>();

    public static void setAction(Action actionVal) {
        action.set(actionVal);
        log.info(String.format("Action %s set", actionVal));
    }

    public static Action getAction() {
        return action.get();
    }

    public static void remove() {
        action.remove();
    }

    public static void main(String[] args){
        Collection<Integer> intCollection = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8);

        Iterable<List<Integer>> subSets = Iterables.partition(intCollection, 2);

        List<Integer> firstPartition = subSets.iterator().next();
        List<Integer> expectedLastPartition = Lists.newArrayList(1, 2, 3);

    }
}
