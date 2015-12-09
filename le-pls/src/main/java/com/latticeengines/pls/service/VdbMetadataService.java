package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;

public interface VdbMetadataService {

    List<VdbMetadataField> getFields(Tenant tenant);

    String getSourceToDisplay(String source);

    void UpdateField(Tenant tenant, VdbMetadataField field);

    void UpdateFields(Tenant tenant, List<VdbMetadataField> fields);

    boolean isLoadGroupRunning(Tenant tenant, String groupName);

    void executeLoadGroup(Tenant tenant, String groupName);
}