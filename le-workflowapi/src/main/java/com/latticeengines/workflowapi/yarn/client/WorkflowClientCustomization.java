package com.latticeengines.workflowapi.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.yarn.client.SingleContainerClientCustomization;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("workflowClientCustomization")
public class WorkflowClientCustomization extends SingleContainerClientCustomization {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(WorkflowClientCustomization.class);

    @Autowired
    public WorkflowClientCustomization(Configuration yarnConfiguration,
            SoftwareLibraryService softwareLibraryService,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${dataplatform.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, softwareLibraryService, hdfsJobBaseDir, webHdfs);
    }

    @Override
    public String getModuleName() {
        return "workflowapi";
    }

}
