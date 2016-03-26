package com.latticeengines.network.exposed.eai;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;

public interface EaiInterface {

    AppSubmission createImportDataJob(ImportConfiguration importConfig);

    AppSubmission createExportDataJob(ExportConfiguration exportConfig);
}
