package com.latticeengines.camille.exposed.interfaces.data;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;

abstract class DataInterfaceBase {
    protected static final long initVersion = 0;

    private final Path basePath;
    private final String interfaceName;

    protected DataInterfaceBase(String interfaceName, CustomerSpace space) throws Exception {
        this.interfaceName = interfaceName;
        this.basePath = PathBuilder.buildDataInterfacePath(CamilleEnvironment.getPodId(), interfaceName, initVersion,
                space.getContractId(), space.getTenantId(), space.getSpaceId());
    }

    protected Path getBasePath() {
        // return a deep copy so children can safely call append
        return new Path(basePath.toString());
    }

    public String getInterfaceName() {
        return interfaceName;
    }
}
