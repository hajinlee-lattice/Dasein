package com.latticeengines.proxy.exposed.sqoop;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

// there is no sqoop server any more
@Deprecated
@Component("sqoopProxy")
public class SqoopProxy extends BaseRestApiProxy {

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private ApplicationContext appCtx;

    private boolean hostPortSwitched;

    public SqoopProxy() {
        super(PropertyUtils.getProperty("common.sqoop.url"), "/sqoop/jobs");
    }

    public AppSubmission importData(SqoopImporter importer) {
        switchHostPortForEmr();
        String url = constructUrl("/import");
        return post("import-table", url, importer, AppSubmission.class);
    }

    public AppSubmission exportData(SqoopExporter exporter) {
        switchHostPortForEmr();
        String url = constructUrl("/export");
        return post("export-table", url, exporter, AppSubmission.class);
    }

    private void switchHostPortForEmr() {
        if (!hostPortSwitched && Boolean.TRUE.equals(useEmr)) {
            synchronized (this) {
                if (!hostPortSwitched && Boolean.TRUE.equals(useEmr)) {
                    EMRService emrService = appCtx.getBean("emrService", EMRService.class);
                    setHostport(emrService.getSqoopHostPort());
                    hostPortSwitched = true;
                }
            }
        }
    }

}
