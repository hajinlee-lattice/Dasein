package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CampaignLaunchInitStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("campaignLaunchInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignLaunchInitStep extends BaseSparkSQLStep<CampaignLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchInitStep.class);

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private CampaignLaunchProcessor campaignLaunchProcessor;

    @Autowired
    private PlayProxy playProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    private DataCollection.Version version;
    private String evaluationDate;
    private AttributeRepository attrRepo;

    @Override
    public void execute() {
        CampaignLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        version = parseDataCollectionVersion(configuration);
        attrRepo = parseAttrRepo(configuration);
        evaluationDate = parseEvaluationDateStr(configuration);

        try {
            log.info("Inside CampaignLaunchInitStep execute()");
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info(String.format("For tenant: %s", customerSpace.toString()));
            log.info(String.format("For playId: %s", playName));
            log.info(String.format("For playLaunchId: %s", playLaunchId));

            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            Map<ExportEntity, HdfsDataUnit> resultMap = retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                }
                Map<ExportEntity, HdfsDataUnit> resultForCurrentAttempt = new HashMap<>();
                try {
                    startSparkSQLSession(getHdfsPaths(attrRepo));
                    /*
                     * 1. form front end query 2. get dataframe for account and
                     * contact respectively 3. generate avro out of dataframe
                     * with predefined format for recommendation
                     */
                } finally {
                    stopSparkSQLSession();
                }

                /*
                 * 4. export to mysql database using sqoop
                 */
                return resultForCurrentAttempt;
            });

            putObjectInContext(ATLAS_EXPORT_DATA_UNIT, resultMap);

            String recAvroHdfsFilePath = campaignLaunchProcessor.launchPlay(tenant, config);
            putStringValueInContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH,
                    recAvroHdfsFilePath);

            successUpdates(customerSpace, playName, playLaunchId);
        } catch (Exception ex) {
            failureUpdates(customerSpace, playName, playLaunchId, ex);
            throw new LedpException(LedpCode.LEDP_18157, ex);
        }
    }

    private void failureUpdates(CustomerSpace customerSpace, String playName, String playLaunchId, Exception ex) {
        log.error(ex.getMessage(), ex);
        playProxy.updatePlayLaunch(customerSpace.toString(), playName, playLaunchId, LaunchState.Failed);
    }

    private void successUpdates(CustomerSpace customerSpace, String playName, String playLaunchId) {
        playProxy.updatePlayLaunch(customerSpace.toString(), playName, playLaunchId, LaunchState.Launched);
        playProxy.publishTalkingPoints(customerSpace.toString(), playName);
    }

    @VisibleForTesting
    void setTenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
        this.tenantEntityMgr = tenantEntityMgr;
    }

    @VisibleForTesting
    void setPlayProxy(PlayProxy playProxy) {
        this.playProxy = playProxy;
    }

    @VisibleForTesting
    void setCampaignLaunchProcessor(CampaignLaunchProcessor campaignLaunchProcessor) {
        this.campaignLaunchProcessor = campaignLaunchProcessor;
    }

    @Override
    protected CustomerSpace parseCustomerSpace(CampaignLaunchInitStepConfiguration stepConfiguration) {
        if (customerSpace == null) {
            customerSpace = configuration.getCustomerSpace();
        }
        return customerSpace;
    }

    @Override
    protected Version parseDataCollectionVersion(CampaignLaunchInitStepConfiguration stepConfiguration) {
        if (version == null) {
            version = configuration.getDataCollectionVersion();
        }
        return version;
    }

    @Override
    protected String parseEvaluationDateStr(CampaignLaunchInitStepConfiguration stepConfiguration) {
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(parseCustomerSpace(stepConfiguration).toString());
        }
        return evaluationDate;
    }

    @Override
    protected AttributeRepository parseAttrRepo(CampaignLaunchInitStepConfiguration stepConfiguration) {
        AttributeRepository attrRepo = WorkflowStaticContext.getObject(ATTRIBUTE_REPO, AttributeRepository.class);
        if (attrRepo == null) {
            throw new RuntimeException("Cannot find attribute repo in context");
        }
        return attrRepo;
    }

}
