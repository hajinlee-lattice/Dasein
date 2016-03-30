package com.latticeengines.proxy.exposed.propdata;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.network.exposed.propdata.SqoopInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class SqoopProxy extends BaseRestApiProxy implements SqoopInterface {

    public SqoopProxy() {
        super("propdata/sqoop");
    }

    @Override
    public AppSubmission importTable(SqoopImporter importer) {
        String url = constructUrl("/imports");
        return post("import", url, importer, AppSubmission.class);
    }


    @Override
    public AppSubmission exportTable(SqoopExporter exporter) {
        String url = constructUrl("/exports");
        return post("export", url, exporter, AppSubmission.class);
    }

}