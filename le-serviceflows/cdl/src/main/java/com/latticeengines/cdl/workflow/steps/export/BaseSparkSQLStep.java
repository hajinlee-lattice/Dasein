package com.latticeengines.cdl.workflow.steps.export;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeRuleRatingsConfig;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.service.RatingQueryService;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.query.exposed.service.SparkSQLService;
import com.latticeengines.query.factory.SparkQueryProvider;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.spark.exposed.job.cdl.MergeRuleRatings;
import com.latticeengines.spark.exposed.service.LivySessionService;

public abstract class BaseSparkSQLStep<S extends BaseStepConfiguration> extends BaseSparkStep<S> {

    private static final Logger log = LoggerFactory.getLogger(BaseSparkSQLStep.class);
    private static final String SQL_USER = SparkQueryProvider.SPARK_BATCH_USER;

    @Inject
    private SparkSQLService sparkSQLService;

    @Inject
    private LivySessionService livySessionService;

    @Inject
    private EntityQueryService entityQueryService;

    @Inject
    private RatingQueryService ratingQueryService;

    @Resource(name = "eventQueryServiceSparkSQL")
    private EventQueryService eventQueryService;

    @Inject
    private TenantService tenantService;

    @Inject
    protected MetadataProxy metadataProxy;

    private LivySession livySession;

    protected abstract CustomerSpace parseCustomerSpace(S stepConfiguration);

    protected abstract DataCollection.Version parseDataCollectionVersion(S stepConfiguration);

    protected abstract String parseEvaluationDateStr(S stepConfiguration);

    protected abstract AttributeRepository parseAttrRepo(S stepConfiguration);

    protected Map<String, String> getHdfsPaths(AttributeRepository attrRepo) {
        String customer = CustomerSpace.shortenCustomerSpace(parseCustomerSpace(configuration).toString());
        Map<String, String> pathMap = new HashMap<>();
        attrRepo.getTableNames().forEach(tblName -> {
            Table table = metadataProxy.getTable(customer, tblName);
            if (table == null) {
                throw new RuntimeException("Table " + tblName + " for customer " + customer + " does not exits.");
            }
            String path = PathUtils.toParquetOrAvroDir(table.getExtracts().get(0).getPath());
            pathMap.put(tblName, path);
        });
        return pathMap;
    }

    protected void startSparkSQLSession(Map<String, String> hdfsPathMap) {
        AttributeRepository attrRepo = parseAttrRepo(configuration);
        QueryServiceUtils.setAttrRepo(attrRepo);
        QueryServiceUtils.toLocalAttrRepoMode();
        double totalSizeInGb = hdfsPathMap.values().stream() //
                .mapToDouble(path -> ScalingUtils.getHdfsPathSizeInGb(yarnConfiguration, path)) //
                .sum();
        int scalingMultiplier = ScalingUtils.getMultiplier(totalSizeInGb);
        livySession = sparkSQLService.initializeLivySession(QueryServiceUtils.getAttrRepo(), hdfsPathMap, //
                scalingMultiplier, false, getClass().getSimpleName());
    }

    protected HdfsDataUnit getEntityQueryData(FrontEndQuery frontEndQuery) {
        setCustomerSpace();

        frontEndQuery.setEvaluationDateStr(parseEvaluationDateStr(configuration));
        frontEndQuery.setPageFilter(null);

        AttributeRepository attrRepo = parseAttrRepo(configuration);
        Map<String, Map<Long, String>> decodeMapping = entityQueryService.getDecodeMapping(attrRepo,
                frontEndQuery.getLookups());

        DataCollection.Version version = parseDataCollectionVersion(configuration);
        String sql = entityQueryService.getQueryStr(frontEndQuery, version, SQL_USER, false);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + ctx.getRetryCount() + ") get SparkSQL data.");
            }
            return sparkSQLService.getData(customerSpace, livySession, sql, decodeMapping);
        });
    }

    protected HdfsDataUnit getEventScoringTarget(EventFrontEndQuery frontEndQuery) {
        setCustomerSpace();
        frontEndQuery.setEvaluationDateStr(parseEvaluationDateStr(configuration));
        frontEndQuery.setPageFilter(null);
        DataCollection.Version version = parseDataCollectionVersion(configuration);
        String sql = eventQueryService.getQueryStr(frontEndQuery, EventType.Scoring, version);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + ctx.getRetryCount() + ") get SparkSQL data.");
            }
            return sparkSQLService.getData(customerSpace, livySession, sql, Collections.emptyMap());
        });
    }

    protected HdfsDataUnit getRuleBasedRatings(FrontEndQuery frontEndQuery, String defaultBkt) {
        setCustomerSpace();
        frontEndQuery.setEvaluationDateStr(parseEvaluationDateStr(configuration));
        frontEndQuery.setPageFilter(null);
        DataCollection.Version version = parseDataCollectionVersion(configuration);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);

        Map<String, String> sqlMap = ratingQueryService.getSparkSQLRuleBasedQueries(frontEndQuery, version);
        List<DataUnit> bktResults = new ArrayList<>();
        List<String> bktNames = new ArrayList<>();
        String defaultSql = sqlMap.get("default");
        HdfsDataUnit defaultResult = retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("(Attempt=" + ctx.getRetryCount() + ") get default ratings via SparkSQL.");
            }
            return sparkSQLService.getData(customerSpace, livySession, defaultSql, null);
        });
        bktResults.add(defaultResult);
        for (RatingBucketName bucketName: RatingBucketName.values()) {
            String bktSql = sqlMap.get(bucketName.getName());
            if (StringUtils.isNotBlank(bktSql)) {
                HdfsDataUnit bktResult = retry.execute(ctx -> {
                    if (ctx.getRetryCount() > 0) {
                        log.info("(Attempt=" + ctx.getRetryCount() + ") get " + //
                                bucketName.getName() + " ratings via SparkSQL.");
                    }
                    return sparkSQLService.getData(customerSpace, livySession, bktSql, null);
                });
                bktResults.add(bktResult);
                bktNames.add(bucketName.getName());
            }
        }

        MergeRuleRatingsConfig jobConfig = new MergeRuleRatingsConfig();
        jobConfig.setInput(bktResults);
        jobConfig.setDefaultBucketName(defaultBkt);
        jobConfig.setBucketNames(bktNames);
        String workspace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
        jobConfig.setWorkspace(workspace);
        SparkJobResult result = runSparkJob(livySession, MergeRuleRatings.class, jobConfig);
        return result.getTargets().get(0);
    }

    protected void stopSparkSQLSession() {
        if (livySession != null) {
            livySessionService.stopSession(livySession);
        }
    }

    private void setCustomerSpace() {
        if (customerSpace == null) {
            customerSpace = parseCustomerSpace(configuration);
        }
        if (MultiTenantContext.getTenant() == null) {
            Tenant tenant = tenantService.findByTenantId(customerSpace.toString());
            if (tenant == null) {
                tenant = tenantService.findByTenantId(customerSpace.getTenantId());
            }
            if (tenant != null) {
                MultiTenantContext.setTenant(tenant);
            } else {
                throw new RuntimeException("Cannot set multi-tenant context for customer " + customerSpace.toString());
            }
        }
    }

}
