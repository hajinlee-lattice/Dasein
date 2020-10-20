package com.latticeengines.dcp.workflow.steps;

import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.OUTPUT_FIELDS;
import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.RAW_USAGE_DISPLAY_NAMES;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.USAGE_CSV_DATA_UNIT;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.idaas.SubscriberDetails;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.AnalyzeUsageConfig;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.security.service.IDaaSService;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.dcp.AnalyzeUsageJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AnalyzeUsage extends RunSparkJob<ImportSourceStepConfiguration, AnalyzeUsageConfig> {

    private static final Logger log = LoggerFactory.getLogger(AnalyzeUsage.class);

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private IDaaSService iDaaSService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

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
        if (projectDetails.getPurposeOfUse() != null) {
            jobConfig.setDRTAttr(projectDetails.getPurposeOfUse().getDomain().getDisplayName() //
                    + "-" + projectDetails.getPurposeOfUse().getRecordType().getDisplayName());
        } else {
            log.info("No purpose of use found for project {}", configuration.getProjectId());
        }

        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());
        if (tenant != null && tenant.getSubscriberNumber() != null) {
            String subNumber = tenant.getSubscriberNumber();
            jobConfig.setSubscriberNumber(subNumber);
            SubscriberDetails subscriberDetails = iDaaSService.getSubscriberDetails(subNumber);
            if (subscriberDetails != null) {
                jobConfig.setSubscriberName(subscriberDetails.getCompanyName());
                jobConfig.setSubscriberCountry(subscriberDetails.getAddress().getCountryCode());
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
                Date startDate = subscriberDetails.getEffectiveDate();
                if (startDate == null) {
                    startDate = new Date();
                }
                jobConfig.setContractStartTime(dateFormat.format(startDate));
                Date endDate = subscriberDetails.getExpirationDate();
                if (endDate == null) {
                    endDate = new Date();
                }
                jobConfig.setContractEndTime(dateFormat.format(endDate));
            } else {
                log.info("No subscriber detail found for {}", subNumber);
            }
        }

        log.info("JobConfig=" + JsonUtils.serialize(jobConfig));
        return jobConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        HdfsDataUnit usageReportDataUnit = result.getTargets().get(0);
        WorkflowStaticContext.putObject(USAGE_CSV_DATA_UNIT, usageReportDataUnit);
    }

}
