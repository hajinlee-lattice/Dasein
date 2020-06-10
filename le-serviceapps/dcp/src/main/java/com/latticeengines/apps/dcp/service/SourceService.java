package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

public interface SourceService {

    Source createSource(String customerSpace, String displayName, String projectId,
                        FieldDefinitionsRecord fieldDefinitionsRecord);

    Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
                        FieldDefinitionsRecord fieldDefinitionsRecord);

    Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
                        String fileImportId, FieldDefinitionsRecord fieldDefinitionsRecord);

    Source updateSource(String customerSpace, String displayName, String sourceId, String fileImportId,
                        FieldDefinitionsRecord fieldDefinitionsRecord);

    Source getSource(String customerSpace, String sourceId);

    Boolean deleteSource(String customerSpace, String sourceId);

    List<Source> getSourceList(String customerSpace, String projectId);

    Boolean pauseSource(String customerSpace, String sourceId);

    Boolean reactivateSource(String customerSpace, String sourceId);
}
