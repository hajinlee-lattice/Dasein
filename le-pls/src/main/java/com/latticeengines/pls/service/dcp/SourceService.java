package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.UpdateSourceRequest;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;

public interface SourceService {

    Source createSource(SourceRequest sourceRequest);

    Source updateSource(String sourceId, UpdateSourceRequest updateSourceRequest);

    Source getSource(String sourceId);

    FieldDefinitionsRecord getSourceMappings(String sourceId);

    List<Source> getSourceList(String projectId);

    Boolean deleteSource(String sourceId);

    Boolean pauseSource(String sourceId);

    FetchFieldDefinitionsResponse fetchFieldDefinitions(String sourceId, String entityType,
                                                        String importFile) throws Exception;

    ValidateFieldDefinitionsResponse validateFieldDefinitions(String importFile,
                                                              ValidateFieldDefinitionsRequest validateRequest);
}
