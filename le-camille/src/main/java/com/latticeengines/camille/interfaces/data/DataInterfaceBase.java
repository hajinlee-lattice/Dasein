package com.latticeengines.camille.interfaces.data;

import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;

abstract class DataInterfaceBase {
    protected static final long initVersion = 0;

    private final Path basePath;
    private final String interfaceName;

    protected DataInterfaceBase(String interfaceName, CustomerSpace space) throws Exception {
        this.interfaceName = interfaceName;
        basePath = PathBuilder.buildDataInterfacePath(CamilleEnvironment.getPodId(), interfaceName, initVersion,
                space.getContractId(), space.getTenantId(), space.getSpaceId());

        Camille c = CamilleEnvironment.getCamille();
        if (!c.exists(basePath)) {
            c.createWithEmptyIntermediateNodes(basePath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
    }

    protected Path getBasePath() {
        // return a deep copy so children can safely call append
        return new Path(basePath.toString());
    }

    public String getInterfaceName() {
        return interfaceName;
    }
}
