package com.latticeengines.camille.lifecycle;

import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Pod;

public class PodLifecycleManager {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(Pod pod) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();

        camille.create(PathBuilder.buildPodPath(pod.getPodId()), ZooDefs.Ids.READ_ACL_UNSAFE);
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
