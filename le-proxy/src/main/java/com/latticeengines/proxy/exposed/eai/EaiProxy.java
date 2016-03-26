package com.latticeengines.proxy.exposed.eai;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.network.exposed.eai.EaiInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("eaiProxy")
public class EaiProxy extends BaseRestApiProxy implements EaiInterface {

    public EaiProxy() {
        super("eai");
    }

    @Override
    public AppSubmission createImportDataJob(ImportConfiguration importConfig) {
        String url = constructUrl("/importjobs");
        return post("createImportDataJob", url, importConfig, AppSubmission.class);
    }

    @Override
    public AppSubmission createExportDataJob(ExportConfiguration exportConfig) {
        String url = constructUrl("/exportjobs");
        return post("createExportDataJob", url, exportConfig, AppSubmission.class);
    }

}
