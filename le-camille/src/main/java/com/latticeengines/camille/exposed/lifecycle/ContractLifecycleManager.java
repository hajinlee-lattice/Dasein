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
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;

public class ContractLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public static void create(String contractId, ContractInfo contractInfo) throws Exception {
        LifecycleUtils.validateIds(contractId);

        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path contractsPath = PathBuilder.buildContractsPath(CamilleEnvironment.getPodId());
            camille.create(contractsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Contracts path @ {}", contractsPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path contractPath = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        try {
            camille.create(contractPath, ZooDefs.Ids.OPEN_ACL_UNSAFE, false);
            log.debug("created Contract @ {}", contractPath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Contract already existed @ {}, ignoring create", contractPath);
        }

        Document properties = DocumentUtils.toRawDocument(contractInfo.properties);
        Path propertiesPath = contractPath.append(PathConstants.PROPERTIES_FILE);
        camille.upsert(propertiesPath, properties, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        log.debug("created properties @ {}", propertiesPath);
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

    public static ContractInfo getInfo(String contractId) throws Exception {
        LifecycleUtils.validateIds(contractId);
        Camille c = CamilleEnvironment.getCamille();

        Path contractPath = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        Document contractPropertiesDocument = c.get(contractPath.append(PathConstants.PROPERTIES_FILE));
        ContractProperties properties = DocumentUtils.toTypesafeDocument(contractPropertiesDocument, ContractProperties.class);

        ContractInfo contractInfo = new ContractInfo(properties);
        return contractInfo;
    }

    public static List<Pair<String, ContractInfo>> getAll() throws IllegalArgumentException, Exception {
        List<Pair<String, ContractInfo>> toReturn = new ArrayList<Pair<String, ContractInfo>>();

        Camille c = CamilleEnvironment.getCamille();
        List<Pair<Document, Path>> childPairs = c.getChildren(PathBuilder.buildContractsPath(CamilleEnvironment
                .getPodId()));

        for (Pair<Document, Path> childPair : childPairs) {
            toReturn.add(new MutablePair<String, ContractInfo>(childPair.getRight().getSuffix(), getInfo(childPair
                    .getRight().getSuffix())));
        }

        return toReturn;
    }

}
