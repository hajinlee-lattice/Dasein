package com.latticeengines.camille.lifecycle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.camille.paths.PathConstants;
import com.latticeengines.domain.exposed.camille.Contract;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentMetadata;
import com.latticeengines.domain.exposed.camille.Path;

public class ContractLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    // thread safe per http://wiki.fasterxml.com/JacksonBestPracticesPerformance
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void create(Contract contract) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path contractsPath = PathBuilder.buildContractsPath(contract.getPodId());
            camille.create(contractsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Contracts path @ {}", contractsPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path contractPath = PathBuilder.buildContractPath(contract.getPodId(), contract.getContractId());
        try {
            camille.create(contractPath, toDocument(contract), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Pod @ {}", contractPath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Contract already existed @ {}, ignoring create", contractPath);
        }
    }

    public static void delete(String podId, String contractId) throws Exception {
        Path contractPath = PathBuilder.buildContractPath(podId, contractId);
        try {
            CamilleEnvironment.getCamille().delete(contractPath);
            log.debug("deleted Contract @ {}", contractPath);
        } catch (KeeperException.NoNodeException e) {
            log.debug("No Contract Existed @ {}, ignoring delete", contractPath);
        }
    }

    public static boolean exists(String podId, String contractId) throws Exception {
        return CamilleEnvironment.getCamille().exists(PathBuilder.buildContractPath(podId, contractId));
    }

    public static Contract get(String podId, String contractId) throws Exception {
        return toContract(CamilleEnvironment.getCamille().get(PathBuilder.buildContractPath(podId, contractId)));
    }

    public static List<Contract> getAll(String podId) throws IllegalArgumentException, Exception {
        List<Pair<Document, Path>> childPairs = CamilleEnvironment.getCamille().getChildren(
                new Path("/" + PathConstants.PODS + "/" + podId + "/" + PathConstants.CONTRACTS));
        Collections.sort(childPairs, new Comparator<Pair<Document, Path>>() {
            @Override
            public int compare(Pair<Document, Path> o1, Pair<Document, Path> o2) {
                return o1.getRight().toString().compareTo(o2.getRight().toString());
            }
        });
        List<Contract> out = new ArrayList<Contract>(childPairs.size());
        for (Pair<Document, Path> childPair : childPairs) {
            out.add(toContract(childPair.getLeft()));
        }
        return out;
    }

    private static Document toDocument(Contract contract) throws JsonProcessingException {
        return new Document(mapper.writeValueAsString(contract), new DocumentMetadata());
    }

    private static Contract toContract(Document doc) throws JsonParseException, JsonMappingException, IOException {
        return mapper.readValue(doc.getData(), Contract.class);
    }
}
