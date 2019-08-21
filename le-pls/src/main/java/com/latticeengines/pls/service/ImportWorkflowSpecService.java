package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;

public interface ImportWorkflowSpecService {

    ImportWorkflowSpec loadSpecFromS3(String systemType, String systemObject);

    Table tableFromSpec(ImportWorkflowSpec spec);
}
