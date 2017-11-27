package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public interface CDLExternalSystemService {

    List<CDLExternalSystem> getAllExternalSystem(String customerSpace);

    void createExternalSystem(String customerSpace, String systemType, InterfaceName accountInterface);

    void createExternalSystem(String customerSpace, InterfaceName crmAccountInt, InterfaceName mapAccountInt,
                              InterfaceName erpAccountInt);
}
