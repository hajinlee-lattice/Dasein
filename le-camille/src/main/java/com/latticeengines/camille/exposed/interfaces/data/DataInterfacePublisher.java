package com.latticeengines.camille.exposed.interfaces.data;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class DataInterfacePublisher extends DataInterfaceBase {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    public DataInterfacePublisher(String interfaceName, CustomerSpace space) throws Exception {
        super(interfaceName, space);
    }

    public void publish(Path localPath, Document doc) throws Exception {
        Camille c = CamilleEnvironment.getCamille();
        Path path = getBasePath().append(localPath);
        try {
            c.create(path, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Node already existed @ {}, forcing update", path);
            c.set(path, doc, true);
        }
    }

    public void remove(Path localPath) throws Exception {
        CamilleEnvironment.getCamille().delete(getBasePath().append(localPath));
    }
}
