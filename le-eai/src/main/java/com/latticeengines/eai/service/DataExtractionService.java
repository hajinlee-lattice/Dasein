package com.latticeengines.eai.service;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;

public interface DataExtractionService {

    void extractAndImport(ImportConfiguration importConfig, ImportContext context);
}
