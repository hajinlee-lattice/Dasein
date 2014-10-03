package com.latticeengines.camille.lifecycle;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.Pod;

public class PodLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(Pod pod) {
        // final Camille camille = CamilleEnvironment.getCamille();

        // camille.create(path, doc, acls)
    }

    public static void delete(String podId) {

    }

    public static void exists(String podId) {

    }

    public static Pod get(String podId) {
        return null;
    }

    public static List<Pod> getAll() {
        return null;
    }
}
