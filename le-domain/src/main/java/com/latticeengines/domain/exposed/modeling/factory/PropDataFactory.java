package com.latticeengines.domain.exposed.modeling.factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.ModelingParameters;

public class PropDataFactory {

    private static final Logger log = LoggerFactory.getLogger(PropDataFactory.class);

    public static final String PROPDATA_NAME_KEY = "propdata.name";

    public static void configPropData(SelectedConfig config, ModelingParameters parameters) {
        log.info("Check and Config PropData.");
        if (config == null || config.getPropData() == null) {
            return;
        }
        PropData propData = config.getPropData();
        if (propData.getPredefinedSelectionName() != null) {
            parameters.setPredefinedSelectionName(propData.getPredefinedSelectionName());
        }
        if (propData.getDataCloudVersion() != null) {
            parameters.setDataCloudVersion(propData.getDataCloudVersion());
        }
        parameters.setExcludePropDataColumns(propData.isExcludePropDataColumns());
        parameters.setExcludePublicDomains(propData.isExcludePublicDomains());

        log.info("Successfully configured the propData");
    }

}
