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

public class SpaceLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(String contractId, String tenantId, String spaceId) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path spacesPath = PathBuilder.buildCustomerSpacesPath(CamilleEnvironment.getPodId(), contractId, tenantId);
            camille.create(spacesPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Spaces path @ {}", spacesPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path spacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                spaceId);
        try {
            camille.create(spacePath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Space @ {}", spacePath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Space already existed @ {}, ignoring create", spacePath);
        }
    }

    public static void delete(String contractId, String tenantId, String spaceId) throws Exception {
        Path spacePath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                spaceId);
        try {
            CamilleEnvironment.getCamille().delete(spacePath);
            log.debug("deleted Space @ {}", spacePath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Space Existed @ {}, ignoring delete", spacePath);
        }
    }

    public static boolean exists(String contractId, String tenantId, String spaceId) throws Exception {
        return CamilleEnvironment.getCamille().exists(
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId));
    }

    /**
     * @return A list of spaceIds
     */
    public static List<String> getAll(String contractId, String tenantId) throws IllegalArgumentException, Exception {
        List<Pair<Document, Path>> childPairs = CamilleEnvironment.getCamille().getChildren(
                PathBuilder.buildCustomerSpacesPath(CamilleEnvironment.getPodId(), contractId, tenantId));
        Collections.sort(childPairs, new Comparator<Pair<Document, Path>>() {
            @Override
            public int compare(Pair<Document, Path> o1, Pair<Document, Path> o2) {
                return o1.getRight().getSuffix().compareTo(o2.getRight().getSuffix());
            }
        });
        List<String> out = new ArrayList<String>(childPairs.size());
        for (Pair<Document, Path> childPair : childPairs) {
            out.add(childPair.getRight().getSuffix());
        }
        return out;
    }
}
