package com.latticeengines.camille.exposed.interfaces.data;

import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class DataInterfacePublisher extends DataInterfaceBase {
    public DataInterfacePublisher(String interfaceName, CustomerSpace space) throws Exception {
        super(interfaceName, space);
    }

    public void publish(Path localPath, Document doc) throws Exception {
        Camille c = CamilleEnvironment.getCamille();
        Path path = getBasePath().append(localPath);
        c.upsert(path, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public void remove(Path localPath) throws Exception {
        CamilleEnvironment.getCamille().delete(getBasePath().append(localPath));
    }
}
