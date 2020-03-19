package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public interface SourceService {

    Source createSource(String customerSpace, String displayName, String projectId,
                        SimpleTemplateMetadata templateMetadata);

    Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
                        SimpleTemplateMetadata templateMetadata);

    Source getSource(String customerSpace, String sourceId);

    List<Source> getSourceList(String customerSpace, String projectId);

    Source convertToSource(String customerSpace, DataFeedTask dataFeedTask);
}
