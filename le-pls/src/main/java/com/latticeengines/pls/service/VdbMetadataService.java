package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.VdbMetadataField;

public interface VdbMetadataService {

    List<VdbMetadataField> getFields(String tenantName, String dlUrl);

    String getSourceToDisplay(String source);

    void UpdateField(String tenantName, String dlUrl, VdbMetadataField field);

    void UpdateFields(String tenantName, String dlUrl, List<VdbMetadataField> fields);
}