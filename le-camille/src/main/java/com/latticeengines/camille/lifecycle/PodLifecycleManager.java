package com.latticeengines.camille.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class PodLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(String podId) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path podsPath = PathBuilder.buildPodsPath();
            camille.create(podsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Pods path @ {}", podsPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path podPath = PathBuilder.buildPodPath(podId);
        try {
            camille.create(podPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Pod @ {}", podPath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Pod already existed @ {}, ignoring create", podPath);
        }
    }

    public static void delete(String podId) throws Exception {
        Path podPath = PathBuilder.buildPodPath(podId);
        try {
            CamilleEnvironment.getCamille().delete(podPath);
            log.debug("deleted Pod @ {}", podPath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Pod Existed @ {}, ignoring delete", podPath);
        }
    }

    public static boolean exists(String podId) throws Exception {
        return CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId));
    }

    /**
     * @return A list of podIds.
     */
    public static List<String> getAll() throws IllegalArgumentException, Exception {
        List<Pair<Document, Path>> childPairs = CamilleEnvironment.getCamille()
                .getChildren(PathBuilder.buildPodsPath());
        Collections.sort(childPairs, new Comparator<Pair<Document, Path>>() {
            @Override
            public int compare(Pair<Document, Path> o1, Pair<Document, Path> o2) {
                return o1.getRight().toString().compareTo(o2.getRight().toString());
            }
        });
        List<String> out = new ArrayList<String>(childPairs.size());
        for (Pair<Document, Path> childPair : childPairs) {
            out.add(childPair.getRight().getSuffix());
        }
        return out;
    }
}
