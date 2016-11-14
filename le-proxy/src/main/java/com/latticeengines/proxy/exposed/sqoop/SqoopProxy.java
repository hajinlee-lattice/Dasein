package com.latticeengines.proxy.exposed.sqoop;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("sqoopProxy")
public class SqoopProxy extends BaseRestApiProxy {

    public SqoopProxy() {
        super(PropertyUtils.getProperty("common.test.sqoop.url"), "/sqoop/jobs");
    }

    public AppSubmission importTable(SqoopImporter importer) {
        String url = constructUrl("/import");
        return post("import-table", url, importer, AppSubmission.class);
    }

    public AppSubmission exportTable(SqoopExporter exporter) {
        String url = constructUrl("/export");
        return post("import-table", url, exporter, AppSubmission.class);
    }

}
