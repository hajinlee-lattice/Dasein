package com.latticeengines.dcp.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.OUTPUT_FIELDS;
import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.RAW_USAGE_DISPLAY_NAMES;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.idaas.SubscriberDetails;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.AnalyzeUsageConfig;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.dcp.AnalyzeUsageJob;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AnalyzeUsage extends RunSparkJob<ImportSourceStepConfiguration, AnalyzeUsageConfig> {

    private static final Logger log = LoggerFactory.getLogger(AnalyzeUsage.class);

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private IDaaSService iDaaSService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Value("${datacloud.manage.url}")
    private String url;

    @Value("${datacloud.manage.user}")
    private String user;

    @Value("${datacloud.manage.password.encrypted}")
    private String password;

    private Map<String, String> dataBlockDispNames = new HashMap<>();

    @Override
    protected Class<AnalyzeUsageJob> getJobClz() {
        return AnalyzeUsageJob.class;
    }

    @Override
    protected AnalyzeUsageConfig configureJob(ImportSourceStepConfiguration stepConfiguration) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        MatchCommand matchCommand = getObjectFromContext(MATCH_COMMAND, MatchCommand.class);
        String inputDir = matchCommand.getUsageLocation();
        if (StringUtils.isBlank(inputDir)) {
            throw new RuntimeException("Cannot find match result dir.");
        }
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setName("usage");
        dataUnit.setPath(inputDir);
        dataUnit.setCount(matchCommand.getRowsRequested().longValue());
        AnalyzeUsageConfig jobConfig = new AnalyzeUsageConfig();
        jobConfig.setInput(Collections.singletonList(dataUnit));

        jobConfig.setOutputFields(OUTPUT_FIELDS);
        jobConfig.setRawOutputMap(RAW_USAGE_DISPLAY_NAMES);
        jobConfig.setUploadId(stepConfiguration.getUploadId());

        ProjectDetails projectDetails = projectProxy.getDCPProjectByProjectId(customerSpace.toString(),
                configuration.getProjectId(), Boolean.FALSE, null);
        jobConfig.setDRTAttr(projectDetails.getPurposeOfUse().getDomain() + "-" + projectDetails.getPurposeOfUse().getRecordType());

        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.getTenantId());
        if (tenant != null && tenant.getSubscriberNumber() != null) {
            jobConfig.setSubscriberNumber(tenant.getSubscriberNumber());
            SubscriberDetails subscriberDetails = iDaaSService.getSubscriberDetails(tenant.getSubscriberNumber());
            jobConfig.setSubscriberName(subscriberDetails.getCompanyName());
            jobConfig.setSubscriberCountry(subscriberDetails.getAddress().getCountryCode());
        }

        log.info("JobConfig=" + JsonUtils.serialize(jobConfig));
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String usageReportPath = result.getTargets().get(0).getPath();
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String uploadId = configuration.getUploadId();
        UploadDetails upload = uploadProxy.getUploadByUploadId(customerSpace.toString(), uploadId, Boolean.TRUE);
        UploadConfig uploadConfig = upload.getUploadConfig();
        uploadConfig.setUsageReportFilePath(usageReportPath);
        uploadProxy.updateUploadConfig(customerSpace.toString(), uploadId, uploadConfig);

        log.info("All usage report is under " + usageReportPath);
    }

}
