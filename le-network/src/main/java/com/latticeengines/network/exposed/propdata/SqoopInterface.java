package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;

public interface SqoopInterface {

    AppSubmission importTable(SqoopImporter importer);
    AppSubmission exportTable(SqoopExporter exporter);

}
