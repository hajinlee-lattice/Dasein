package com.latticeengines.domain.exposed.modeling.factory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;
import com.latticeengines.domain.exposed.pls.ModelingParameters;


public class PropDataFactory {

    private static final Log log = LogFactory.getLog(PropDataFactory.class);
    
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
        if (propData.getVersion() != null) {
            parameters.setSelectedVersion(propData.getVersion());
        }
        if (propData.getDataCloudVersion() != null) {
            parameters.setDataCloudVersion(propData.getDataCloudVersion());
        }
        parameters.setExcludePropDataColumns(propData.isExcludePropDataColumns());
        
        log.info("Successfully configured the propData");
    }

}
