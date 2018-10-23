package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface CDLExternalSystemService {

    List<CDLExternalSystem> getAllExternalSystem(String customerSpace);

    CDLExternalSystem getExternalSystem(String customerSpace, BusinessEntity entity);

    void createOrUpdateExternalSystem(String customerSpace, CDLExternalSystem cdlExternalSystem, BusinessEntity entity);

    List<CDLExternalSystemMapping> getExternalSystemByType(String customerSpace, CDLExternalSystemType type,
            BusinessEntity entity);

    Map<String, List<CDLExternalSystemMapping>> getExternalSystemMap(String customerSpace, BusinessEntity entity);
}
