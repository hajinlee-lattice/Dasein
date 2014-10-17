package com.latticeengines.camille.interfaces.data;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class DataInterfacePublisher extends DataInterfaceBase {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public DataInterfacePublisher(String interfaceName) throws Exception {
        super(interfaceName);
    }

    public void publish(Path relativePath, Document doc) throws Exception {
        Camille c = CamilleEnvironment.getCamille();
        Path path = getBasePath().append(relativePath);
        try {
            c.createWithEmptyIntermediateNodes(path, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Node already existed @ {}, forcing update", path);
            c.set(path, doc, true);
        }
    }

    public void remove(Path relativePath) throws Exception {
        CamilleEnvironment.getCamille().delete(getBasePath().append(relativePath));
    }
}
