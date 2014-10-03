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
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentMetadata;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.Pod;

public class PodLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    // thread safe per http://wiki.fasterxml.com/JacksonBestPracticesPerformance
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void create(Pod pod) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path podsPath = PathBuilder.buildPodsPath();
            camille.create(podsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            log.debug("created Pods path @ {}", podsPath);
        } catch (KeeperException.NodeExistsException e) {
        }

        Path podPath = PathBuilder.buildPodPath(pod.getPodId());
        try {
            camille.create(podPath, toDocument(pod), ZooDefs.Ids.OPEN_ACL_UNSAFE);
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

    public static Pod get(String podId) throws Exception {
        return toPod(CamilleEnvironment.getCamille().get(PathBuilder.buildPodPath(podId)));
    }

    public static List<Pod> getAll() throws IllegalArgumentException, Exception {
        List<Pair<Document, Path>> childPairs = CamilleEnvironment.getCamille().getChildren(
                new Path("/" + PathConstants.PODS));
        Collections.sort(childPairs, new Comparator<Pair<Document, Path>>() {
            @Override
            public int compare(Pair<Document, Path> o1, Pair<Document, Path> o2) {
                return o1.getRight().toString().compareTo(o2.getRight().toString());
            }
        });
        List<Pod> out = new ArrayList<Pod>(childPairs.size());
        for (Pair<Document, Path> childPair : childPairs) {
            out.add(toPod(childPair.getLeft()));
        }
        return out;
    }

    private static Document toDocument(Pod pod) throws JsonProcessingException {
        return new Document(mapper.writeValueAsString(pod), new DocumentMetadata());
    }

    private static Pod toPod(Document doc) throws JsonParseException, JsonMappingException, IOException {
        return mapper.readValue(doc.getData(), Pod.class);
    }
}
