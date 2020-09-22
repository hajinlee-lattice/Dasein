package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

public interface CDLDanteConfigProxy {

    DanteConfigurationDocument getDanteConfiguration(String customerSpace);

    void refreshDanteConfiguration(String customerSpace);

}
