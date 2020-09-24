package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface CDLExternalSystemNameService {
    Map<BusinessEntity, List<CDLExternalSystemName>> getAllExternalSystemNames();

    List<CDLExternalSystemName> getExternalSystemNamesForLiveRamp();
}
