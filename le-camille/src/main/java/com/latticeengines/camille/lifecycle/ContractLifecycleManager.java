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

public class ContractLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(String contractId) throws Exception {
        LifecycleUtils.validateIds(contractId);

        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path contractsPath = PathBuilder.buildContractsPath(CamilleEnvironment.getPodId());
            camille.create(contractsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Contracts path @ {}", contractsPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path contractPath = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        try {
            camille.create(contractPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Contract @ {}", contractPath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Contract already existed @ {}, ignoring create", contractPath);
        }
    }

    public static void delete(String contractId) throws Exception {
        LifecycleUtils.validateIds(contractId);

        Path contractPath = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        try {
            CamilleEnvironment.getCamille().delete(contractPath);
            log.debug("deleted Contract @ {}", contractPath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Contract Existed @ {}, ignoring delete", contractPath);
        }
    }

    public static boolean exists(String contractId) throws Exception {
        LifecycleUtils.validateIds(contractId);

        return CamilleEnvironment.getCamille().exists(
                PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId));
    }

    /**
     * @return A list of contractIds
     */
    public static List<String> getAll() throws IllegalArgumentException, Exception {
        List<Pair<Document, Path>> childPairs = CamilleEnvironment.getCamille().getChildren(
                PathBuilder.buildContractsPath(CamilleEnvironment.getPodId()));
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
