package com.latticeengines.eai.exposed.service;

import com.latticeengines.domain.exposed.eai.DataExtractionConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;

public interface DataExtractionService {

    void extractAndImport(DataExtractionConfiguration extractionConfig, ImportContext context);
}
