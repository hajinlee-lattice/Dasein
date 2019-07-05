package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;

public interface ExportFieldMetadataService {

    List<ExportFieldMetadataDefaults> getStandardExportFields(CDLExternalSystemName systemName);

    List<ColumnMetadata> getExportEnabledFields(String customerSpace, PlayLaunchChannel channel);

    Flux<ColumnMetadata> getServingMetadataForEntity(String customerSpace, BusinessEntity entity);
}
