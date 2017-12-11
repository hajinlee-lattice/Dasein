package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;

public interface CDLExternalSystemService {

    List<CDLExternalSystem> getAllExternalSystem(String customerSpace);

    CDLExternalSystem getExternalSystem(String customerSpace);

    void createOrUpdateExternalSystem(String customerSpace, CDLExternalSystem cdlExternalSystem);
}
