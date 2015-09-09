package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.MetadataField;

public interface MetadataService {

    List<MetadataField> getMetadataFields(String tenantName, String dlUrl);

    String getSourceToDisplay(String source);

    void UpdateField(String tenantName, String dlUrl, MetadataField field);

    void UpdateFields(String tenantName, String dlUrl, List<MetadataField> fields);
}