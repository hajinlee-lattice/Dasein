package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;

public interface InternalInterface {

    AppSubmission importTable(SqoopImporter importer);
    AppSubmission exportTable(SqoopExporter exporter);
    AppSubmission submitYarnJob(PropDataJobConfiguration propDataJobConfiguration);

}
