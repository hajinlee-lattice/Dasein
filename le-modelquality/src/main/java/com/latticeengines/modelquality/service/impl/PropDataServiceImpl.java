package com.latticeengines.modelquality.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.service.PropDataService;

@Component("propDataService")
public class PropDataServiceImpl extends BaseServiceImpl implements PropDataService {
    
    @Autowired
    private PropDataEntityMgr propDataEntityMgr;

    @Override
    public PropData createLatestProductionPropData() {
        String version = getVersion();
        String propDataName = "PRODUCTION-" + version;
        PropData propData = propDataEntityMgr.findByName(propDataName);
        
        if(propData != null)
        {
            return propData;
        }
        propData = new PropData();
        propData.setName(propDataName);
        propData.setDataCloudVersion("1.0.0");
        propDataEntityMgr.create(propData);
        return propData;

    }

}
