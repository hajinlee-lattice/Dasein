package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.ModelAlerts;

public interface ModelAlertService {

    ModelAlerts.ModelQualityWarnings generateModelQualityWarnings(String tenantId, String modelId);

    ModelAlerts.MissingMetaDataWarnings generateMissingMetaDataWarnings(String tenantId, String modelId);

}
