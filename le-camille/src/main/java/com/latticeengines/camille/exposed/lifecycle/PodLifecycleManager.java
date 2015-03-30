package com.latticeengines.camille.exposed.lifecycle;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.PodInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.PodProperties;

public class PodLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(String podId, PodInfo podInfo) throws Exception {
        LifecycleUtils.validateIds(podId);

        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path podsPath = PathBuilder.buildPodsPath();
            camille.create(podsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Pods path @ {}", podsPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path podPath = PathBuilder.buildPodPath(podId);
        try {
            camille.create(podPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Pod @ {}", podPath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Pod already existed @ {}, ignoring create", podPath);
        }

        Document properties = DocumentUtils.toDocument(podInfo.properties);
        Path propertiesPath = podPath.append(PathConstants.PROPERTIES_FILE);
        camille.upsert(propertiesPath, properties, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        log.debug("created properties @ {}", propertiesPath);
    }

    public static void delete(String podId) throws Exception {
        LifecycleUtils.validateIds(podId);

        Path podPath = PathBuilder.buildPodPath(podId);
        try {
            CamilleEnvironment.getCamille().delete(podPath);
            log.debug("deleted Pod @ {}", podPath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Pod Existed @ {}, ignoring delete", podPath);
        }
    }

    public static boolean exists(String podId) throws Exception {
        LifecycleUtils.validateIds(podId);

        return CamilleEnvironment.getCamille().exists(PathBuilder.buildPodPath(podId));
    }

    public static PodInfo getInfo(String podId) throws Exception {
        Camille c = CamilleEnvironment.getCamille();

        Path podPath = PathBuilder.buildPodPath(podId);
        Document podPropertiesDocument = c.get(podPath.append(PathConstants.PROPERTIES_FILE));
        PodProperties properties = DocumentUtils.toObject(podPropertiesDocument, PodProperties.class);

        PodInfo podInfo = new PodInfo(properties);
        return podInfo;
    }

    public static List<Pair<String, PodInfo>> getAll() throws IllegalArgumentException, Exception {
        List<Pair<String, PodInfo>> toReturn = new ArrayList<Pair<String, PodInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<Pair<Document, Path>> childPairs = c.getChildren(PathBuilder.buildPodsPath());

        for (Pair<Document, Path> childPair : childPairs) {
            toReturn.add(new MutablePair<String, PodInfo>(childPair.getRight().getSuffix(), getInfo(childPair
                    .getRight().getSuffix())));
        }

        return toReturn;
    }
}
