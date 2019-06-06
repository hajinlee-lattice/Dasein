package com.latticeengines.datacloud.core.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;

public interface SourceAttributeEntityMgr {

    void createAttribute(SourceAttribute attr);
    List<SourceAttribute> getAttributes(String sourceName, String stage, String transform);

    List<SourceAttribute> getAttributes(String sourceName, String stage, String transform, String datacloudVersion,
            boolean isCustomer);

    String getLatestDataCloudVersion(String sourceName, String stage, String transform);

    List<String> getAllDataCloudVersions(String source, String stage, String transformer);
}
