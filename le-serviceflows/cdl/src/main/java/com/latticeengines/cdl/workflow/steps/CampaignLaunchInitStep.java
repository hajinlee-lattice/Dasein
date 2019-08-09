package com.latticeengines.cdl.workflow.steps;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATTRIBUTE_REPO;

import java.util.Arrays;
import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CampaignLaunchInitStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateRecommendationConfig;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.spark.exposed.job.cdl.CreateRecommendationsJob;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("campaignLaunchInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CampaignLaunchInitStep extends BaseSparkSQLStep<CampaignLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchInitStep.class);

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CampaignLaunchProcessor campaignLaunchProcessor;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    protected SparkJobService sparkJobService;

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

            log.info(String.format("For tenant: %s", customerSpace.toString()) + "\n"
                    + String.format("For playId: %s", playName) + "\n"
                    + String.format("For playLaunchId: %s", playLaunchId));

            PlayLaunchContext playLaunchContext = campaignLaunchProcessor.initPlayLaunchContext(tenant, config);

            long totalAccountsAvailableForLaunch = playLaunchContext.getPlayLaunch().getAccountsSelected();
            long totalContactsAvailableForLaunch = playLaunchContext.getPlayLaunch().getContactsSelected();
            log.info(String.format("Total available accounts available for Launch: %d, contacts: %d",
                    totalAccountsAvailableForLaunch, totalContactsAvailableForLaunch));

            // 1. Generate FrontEndQueries for Account and Contact in the PlayLaunchContext
            campaignLaunchProcessor.prepareFrontEndQueries(playLaunchContext, version);

            SparkJobResult createRecJobResult = executeSparkJob(playLaunchContext);

            long launchedAccountNum = createRecJobResult.getTargets().get(0).getCount();
            log.info("Total Account launched: " + launchedAccountNum);
            long launchedContactNum = 0L;
            if (createRecJobResult.getOutput() != null) {
                launchedContactNum = Long.parseLong(createRecJobResult.getOutput());
                log.info("Total Contact launched: " + launchedContactNum);
            } else {
                log.warn("Contact is null");
            }
            log.info(createRecJobResult.getOutput());
            String targetPath = createRecJobResult.getTargets().get(0).getPath();
            log.info("Target HDFS path: " + targetPath);
            putStringValueInContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH,
                    PathUtils.toAvroGlob(targetPath));

            /*
             * 4. export to mysql database using sqoop
             */
            // as per PM requirement, we need to fail play launch if 0
            // recommendations were created.
            if (launchedAccountNum <= 0L) {
                throw new LedpException(LedpCode.LEDP_18159, new Object[] { launchedAccountNum, 0L });
            } else {
                runScoopExport(playLaunchContext, tenant, launchedAccountNum, launchedContactNum,
                        totalAccountsAvailableForLaunch, totalContactsAvailableForLaunch, targetPath);
                successUpdates(customerSpace, playLaunchContext.getPlayName(), playLaunchContext.getPlayLaunchId());
            }
        } catch (Exception ex) {
            failureUpdates(customerSpace, playName, playLaunchId, ex);
            throw new LedpException(LedpCode.LEDP_18157, ex);
        }
    }

    private void runScoopExport(PlayLaunchContext playLaunchContext, Tenant tenant, //
            long launchedAccountNum, long launchedContactNum, //
            long totalAccountsAvailableForLaunch, long totalContactsAvailableForLaunch, String targetPath)
            throws Exception {
        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();
        playLaunch.setAccountsLaunched(launchedAccountNum);
        playLaunch.setContactsLaunched(launchedContactNum);
        campaignLaunchProcessor.runSqoopExportRecommendations(tenant, playLaunchContext, System.currentTimeMillis(),
                targetPath);
        long suppressedAccounts = (totalAccountsAvailableForLaunch - launchedAccountNum);
        playLaunch.setAccountsSuppressed(suppressedAccounts);
        long suppressedContacts = (totalContactsAvailableForLaunch - launchedContactNum);
        playLaunch.setContactsSuppressed(suppressedContacts);
        campaignLaunchProcessor.updateLaunchProgress(playLaunchContext);
        log.info(String.format("Total suppressed account count for launch: %d", suppressedAccounts));
        log.info(String.format("Total suppressed contact count for launch: %d", suppressedContacts));
    }

    private SparkJobResult executeSparkJob(PlayLaunchContext playLaunchContext) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + (ctx.getRetryCount() + 1) + ") extract entities via Spark SQL.");
                log.warn("Previous failure:", ctx.getLastThrowable());
            }
            try {
                startSparkSQLSession(getHdfsPaths(attrRepo), false);

                // 2. get DataFrame for Account and Contact
                HdfsDataUnit accountDataUnit = getEntityQueryData(playLaunchContext.getAccountFrontEndQuery());
                log.info("accountDataUnit: " + JsonUtils.serialize(accountDataUnit));
                HdfsDataUnit contactDataUnit = null;
                String contactTableName = attrRepo.getTableName(TableRoleInCollection.SortedContact);
                if (StringUtils.isBlank(contactTableName)) {
                    log.info("No contact table available in Redshift.");
                } else {
                    contactDataUnit = getEntityQueryData(playLaunchContext.getContactFrontEndQuery());
                    log.info("contactDataUnit: " + JsonUtils.serialize(contactDataUnit));
                }

                // 3. generate avro out of DataFrame with predefined format for Recommendations
                return executeSparkJob(CreateRecommendationsJob.class,
                        generateCreateRecommendationConfig(accountDataUnit, contactDataUnit, playLaunchContext));
            } finally {
                stopSparkSQLSession();
            }
        });
    }

    private CreateRecommendationConfig generateCreateRecommendationConfig(HdfsDataUnit accountDataUnit,
            HdfsDataUnit contactDataUnit, PlayLaunchContext playLaunchContext) {
        CreateRecommendationConfig createRecConfig = new CreateRecommendationConfig();
        createRecConfig.setWorkspace(getRandomWorkspace());
        if (contactDataUnit != null) {
            createRecConfig.setInput(Arrays.asList(accountDataUnit, contactDataUnit));
        } else {
            createRecConfig.setInput(Collections.singletonList(accountDataUnit));
        }
        createRecConfig.setPlayLaunchSparkContext(playLaunchContext.toPlayLaunchSparkContext());
        return createRecConfig;
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
