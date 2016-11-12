package com.latticeengines.network.exposed.modelquality;

import java.util.List;

import com.latticeengines.domain.exposed.modelquality.PropData;

public interface ModelQualityPropDataInterface {

    List<PropData> getPropDataConfigs();

    String createPropDataConfig(PropData propDataConfig);

    PropData createPropDataConfigFromProduction();

    PropData getPropDataConfigByName(String propDataConfigName);
    
    List<PropData> createPropDataConfigFromProductionForUI();
}
