package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class VisitorBook {

    private static final Logger log = LoggerFactory.getLogger(VisitorBook.class);

    private ThreadLocal<Set<Pair<String, String>>> visitorBookThreadLocal;

    @PostConstruct
    public void init() {
        visitorBookThreadLocal = new ThreadLocal<>();
    }

    public void initVisitorBook() {
        cleanupVisitorBook();
        visitorBookThreadLocal.set(new HashSet<>());
    }

    public void cleanupVisitorBook() {
        Set<Pair<String, String>> visitorBook = visitorBookThreadLocal.get();
        if (CollectionUtils.isNotEmpty(visitorBook)) {
            visitorBook.clear();
        }
        visitorBookThreadLocal.set(null);
    }

    public void addVisitEntry(Pair<String, String> visitorEntry) {
        Set<Pair<String, String>> visitorBook = visitorBookThreadLocal.get();
        if (visitorBook != null) {
            log.info(String.format("Making entry for %s in visitor book", visitorEntry));
            visitorBook.add(visitorEntry);
        }
    }

    public boolean hasVisitEntry(Pair<String, String> visitorEntry) {
        Set<Pair<String, String>> visitorBook = visitorBookThreadLocal.get();
        if (visitorBook != null) {
            if (visitorBook.contains(visitorEntry)) {
                log.info(String.format("Entry for %s is already present in visitor book", visitorEntry));
                return true;
            }
        }
        return false;
    }
}
