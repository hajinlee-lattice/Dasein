package com.latticeengines.camille.exposed.lifecycle;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

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

        Document properties = DocumentUtils.toRawDocument(podInfo.properties);
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
        PodProperties properties = DocumentUtils.toTypesafeDocument(podPropertiesDocument, PodProperties.class);

        PodInfo podInfo = new PodInfo(properties);
        return podInfo;
    }

    public static List<AbstractMap.SimpleEntry<String, PodInfo>> getAll() throws IllegalArgumentException, Exception {
        List<AbstractMap.SimpleEntry<String, PodInfo>> toReturn = new ArrayList<>();

        Camille c = CamilleEnvironment.getCamille();
        List<AbstractMap.SimpleEntry<Document, Path>> childPairs = c.getChildren(PathBuilder.buildPodsPath());

        for (AbstractMap.SimpleEntry<Document, Path> childPair : childPairs) {
            try {
                toReturn.add(new AbstractMap.SimpleEntry<String, PodInfo>(childPair.getValue().getSuffix(),
                        getInfo(childPair.getValue().getSuffix())));
            } catch (Exception ex) {
                log.warn("Failed to get Pod Info for "
                        + (childPair.getValue() != null ? childPair.getValue().getSuffix() : ""), ex);
            }
        }

        return toReturn;
    }
}
