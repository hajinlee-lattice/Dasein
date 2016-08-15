package com.latticeengines.proxy.exposed.propdata;

import java.util.Date;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.network.exposed.propdata.InternalInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class InternalProxy extends MicroserviceRestApiProxy implements InternalInterface {

    public InternalProxy() {
        super("propdata/internal");
    }

    @Override
    public AppSubmission importTable(SqoopImporter importer) {
        String url = constructUrl("/sqoopimports");
        return post("import", url, importer, AppSubmission.class);
    }


    @Override
    public AppSubmission exportTable(SqoopExporter exporter) {
        String url = constructUrl("/sqoopexports");
        return post("export", url, exporter, AppSubmission.class);
    }

    @Override
    public AppSubmission submitYarnJob(PropDataJobConfiguration jobConfiguration) {
        String url = constructUrl("/yarnjobs");
        return post("submitYarnJob", url, jobConfiguration, AppSubmission.class);
    }

    @Override
    public Date currentCacheTableVersion() {
        String url = constructUrl("/currentcachetableversion");
        return get("submitYarnJob", url, Date.class);
    }

}