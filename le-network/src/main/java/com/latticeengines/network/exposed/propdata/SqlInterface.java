package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.propdata.ExportRequest;
import com.latticeengines.domain.exposed.propdata.ImportRequest;

public interface SqlInterface {

    AppSubmission importTable(ImportRequest importRequest);
    AppSubmission exportTable(ExportRequest exportRequest);

}
