package com.latticeengines.workflowapi.yarn.client;

import java.util.Collection;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.client.SingleContainerClientCustomization;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

@Component("workflowClientCustomization")
public class WorkflowClientCustomization extends SingleContainerClientCustomization {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(WorkflowClientCustomization.class);

    private VersionManager versionManager;

    @Autowired
    public WorkflowClientCustomization(Configuration yarnConfiguration, VersionManager versionManager,
            SoftwareLibraryService softwareLibraryService,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${dataplatform.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, softwareLibraryService, hdfsJobBaseDir, webHdfs);
        this.versionManager = versionManager;
    }

    @Override
    public String getModuleName() {
        return "workflowapi";
    }

    @Override
    public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
        Collection<TransferEntry> hdfsEntries = super.getHdfsEntries(containerProperties);

        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/workflow/workflow.properties", versionManager.getCurrentVersion()),//
                false));
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/propdata/propdata.properties", versionManager.getCurrentVersion()),//
                false));

        return hdfsEntries;
    }

}
