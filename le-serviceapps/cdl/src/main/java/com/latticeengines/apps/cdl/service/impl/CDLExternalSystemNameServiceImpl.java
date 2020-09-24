package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.CDLExternalSystemNameService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("cdlExternalSystemNameService")
public class CDLExternalSystemNameServiceImpl implements CDLExternalSystemNameService {

    @Override
    public Map<BusinessEntity, List<CDLExternalSystemName>> getAllExternalSystemNames() {
        return CDLExternalSystemName.getEntityMap();
    }

    @Override
    public List<CDLExternalSystemName> getExternalSystemNamesForLiveRamp() {
        return CDLExternalSystemName.LIVERAMP;
    }

}
