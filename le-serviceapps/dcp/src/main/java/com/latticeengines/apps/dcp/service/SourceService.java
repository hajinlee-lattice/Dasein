package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

public interface SourceService {

//    Source createSource(String customerSpace, String displayName, String projectId,
//                        SimpleTemplateMetadata templateMetadata);
//
//    Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
//                        SimpleTemplateMetadata templateMetadata);

    Source createSource(String customerSpace, String displayName, String projectId,
                        FieldDefinitionsRecord fieldDefinitionsRecord);

    Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
                        FieldDefinitionsRecord fieldDefinitionsRecord);

    Source getSource(String customerSpace, String sourceId);

    Boolean deleteSource(String customerSpace, String sourceId);

    List<Source> getSourceList(String customerSpace, String projectId);

    Source convertToSource(String customerSpace, DataFeedTask dataFeedTask);
}
