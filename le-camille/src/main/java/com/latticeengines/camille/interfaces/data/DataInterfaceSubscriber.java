package com.latticeengines.camille.interfaces.data;

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
}
