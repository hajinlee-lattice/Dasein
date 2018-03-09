package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;

public interface CDLExternalSystemService {

    List<CDLExternalSystem> getAllExternalSystem(String customerSpace);

    CDLExternalSystem getExternalSystem(String customerSpace);

    void createOrUpdateExternalSystem(String customerSpace, CDLExternalSystem cdlExternalSystem);

    List<CDLExternalSystemMapping> getExternalSystemByType(String customerSpace, CDLExternalSystemType type);

    Map<String, List<CDLExternalSystemMapping>> getExternalSystemMap(String customerSpace);
}
