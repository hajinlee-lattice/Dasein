package com.latticeengines.apps.core.service;

import java.io.IOException;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

public interface ImportWorkflowSpecService {

    ImportWorkflowSpec loadSpecFromS3(String systemType, String systemObject) throws IOException;

    Table tableFromRecord(String tableName, boolean writeAllDefinitions, FieldDefinitionsRecord record);

    List<ImportWorkflowSpec> loadSpecWithSameObjectExcludeTypeFromS3(String excludeSystemType, String systemObject) throws Exception;

    void putSpecToS3(String systemType, String systemObject, ImportWorkflowSpec importWorkflowSpec) throws Exception;

    void cleanupSpecFromS3(String systemType, String systemObject) throws Exception;
}
