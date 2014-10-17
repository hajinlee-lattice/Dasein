package com.latticeengines.camille.interfaces.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class DataInterfaceSubscriber extends DataInterfaceBase {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public DataInterfaceSubscriber(String interfaceName) throws Exception {
        super(interfaceName);
    }

    /**
     * @return The corresponding Document, or null if no such Document exists.
     */
    public Document get(Path relativePath) throws Exception {
        if (relativePath.numParts() > 1) {
            IllegalArgumentException e = new IllegalArgumentException("relativePath cannot have more than 1 part");
            log.error(e.getMessage(), e);
            throw e;
        }

        try {
            return CamilleEnvironment.getCamille().get(getBasePath().append(relativePath));
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    public List<Pair<Document, Path>> getChildren(Path relativePath) throws Exception {
        List<Pair<Document, Path>> children = CamilleEnvironment.getCamille().getChildren(
                getBasePath().append(relativePath));

        if (children == null)
            return Collections.emptyList();

        List<Pair<Document, Path>> out = new ArrayList<>(children.size());

        for (Pair<Document, Path> child : children) {
            out.add(MutablePair.of(child.getLeft(), child.getRight().local(getBasePath())));
        }

        return out;
    }
}
