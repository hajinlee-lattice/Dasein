package com.latticeengines.objectapi.service.sparksql.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.util.RestrictionOptimizer;
import com.latticeengines.objectapi.service.RatingQuerySparkSQLService;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.service.impl.RatingQueryServiceImpl;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.factory.SparkQueryProvider;

@Service("ratingQueryServiceSparkSQL")
public class RatingQueryServiceSparkSQLImpl extends RatingQueryServiceImpl implements RatingQuerySparkSQLService {

    private static final Logger log = LoggerFactory.getLogger(RatingQueryServiceSparkSQLImpl.class);


    @Inject
    public RatingQueryServiceSparkSQLImpl(@Named("queryEvaluatorServiceSparkSQL") QueryEvaluatorService queryEvaluatorService,
                                          TransactionService transactionService) {
        super(queryEvaluatorService, transactionService);
    }

    /**
     * @param livySession
     *
     * This is added for Testing Purpose. In real world, this session will be created at runtime
     */
    public void setLivySession(LivySession livySession) {
        ((QueryEvaluatorServiceSparkSQL) queryEvaluatorService).setLivySession(livySession);
    }

    @Override
    public Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery, DataCollection.Version version,
                                            String sqlUser) {
        HdfsDataUnit sparkResult = getRatingData(frontEndQuery, version);
        String avroPath = sparkResult.getPath();
        Configuration yarnConfiguration = //
                ((QueryEvaluatorServiceSparkSQL) queryEvaluatorService).getYarnConfiguration();
        try (AvroUtils.AvroFilesIterator iterator = //
                AvroUtils.iterateAvroFiles(yarnConfiguration, PathUtils.toAvroGlob(avroPath))) {
            Map<String, Long> coverage = new TreeMap<>();
            iterator.forEachRemaining(record -> {
                String rating = record.get(InterfaceName.Rating.name()).toString();
                long count = coverage.getOrDefault(rating, 0L);
                coverage.put(rating, count + 1);
            });
            return coverage;
        }
    }

    @Override
    public HdfsDataUnit getRatingData(FrontEndQuery frontEndQuery, DataCollection.Version version) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        List<RatingModel> models = frontEndQuery.getRatingModels();
        if (models == null || models.size() != 1 || !(models.get(0) instanceof RuleBasedModel)) {
            throw new IllegalArgumentException("Should provide one and only one rule based rating model");
        }
        RuleBasedModel model = (RuleBasedModel) models.get(0);
        String defaultBkt = model.getRatingRule().getDefaultBucketName();
        List<Pair<String, String>> ruleSqls = getRuleQueries(frontEndQuery, model, version);
        List<Pair<String, String>> nonEmptyRuleSqls = new ArrayList<>();
        List<String> viewList = new ArrayList<>();
        for (Pair<String, String> pair: ruleSqls) {
            String alias = pair.getLeft();
            String sql = pair.getRight();
            if (StringUtils.isNotBlank(sql)) {
                nonEmptyRuleSqls.add(pair);
                viewList.add(alias);
            }
        }
        QueryEvaluatorServiceSparkSQL serviceSparkSQL = (QueryEvaluatorServiceSparkSQL) queryEvaluatorService;
        List<String> tempViews = serviceSparkSQL.createViews(customerSpace, nonEmptyRuleSqls);
        return serviceSparkSQL.mergeRules(customerSpace, viewList, tempViews, defaultBkt);
    }

    private List<Pair<String, String>> getRuleQueries(FrontEndQuery frontEndQuery, RuleBasedModel model, //
                                                     DataCollection.Version version) {
        frontEndQuery.setRatingModels(null);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setPageFilter(null);
        frontEndQuery.setSort(null); // always sort by rating
        List<Pair<String, String>> sqls = new ArrayList<>();
        FrontEndQuery defaultQuery = setRatingLookup(frontEndQuery.getDeepCopy(), "Z");
        String defaultRatingSql = getQueryStr(defaultQuery, version, SparkQueryProvider.SPARK_BATCH_USER);
        sqls.add(Pair.of("DefaultBucket", defaultRatingSql));
        for (RatingBucketName bucketName: RatingBucketName.values()) {
            // in natural order of the enum
            Map<String, Restriction> rules = model.getRatingRule().getRuleForBucket(bucketName);
            FrontEndQuery ruleQuery = mergeRules(frontEndQuery, rules);
            if (ruleQuery != null) {
                setRatingLookup(ruleQuery, bucketName.getName());
                String bucketSql = getQueryStr(ruleQuery, version, SparkQueryProvider.SPARK_BATCH_USER);
                sqls.add(Pair.of("Bucket" + bucketName.getName(), bucketSql));
            }
        }
        return sqls;
    }

    private static FrontEndQuery setRatingLookup(FrontEndQuery query, String bucketName) {
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
        ValueLookup ratingLookup = new ValueLookup(bucketName, InterfaceName.Rating.name());
        query.setLookups(Arrays.asList(attrLookup, ratingLookup));
        return query;
    }

    private static FrontEndQuery mergeRules(FrontEndQuery query, Map<String, Restriction> rules) {
        FrontEndQuery mergedQuery = null;
        if (MapUtils.isNotEmpty(rules)) {
            Restriction accRestInRule = //
                    RestrictionOptimizer.optimize(rules.get(FrontEndQueryConstants.ACCOUNT_RESTRICTION));
            Restriction ctcRestInRule = //
                    RestrictionOptimizer.optimize(rules.get(FrontEndQueryConstants.CONTACT_RESTRICTION));
            if (accRestInRule != null) {
                accRestInRule = accRestInRule.getDeepCopy();
            }
            if (ctcRestInRule != null) {
                ctcRestInRule = ctcRestInRule.getDeepCopy();
            }
            if (accRestInRule != null || ctcRestInRule != null) {
                mergedQuery = query.getDeepCopy();
                Restriction accRestInQuery = mergedQuery.getAccountRestriction() == null ? //
                        null : mergedQuery.getAccountRestriction().getRestriction();
                Restriction accRest = mergeRestrictions(accRestInQuery, accRestInRule);
                mergedQuery.setAccountRestriction(new FrontEndRestriction(accRest));
                Restriction ctcRestInQuery = mergedQuery.getContactRestriction() == null ? //
                        null : mergedQuery.getContactRestriction().getRestriction();
                Restriction ctcRest = mergeRestrictions(ctcRestInQuery, ctcRestInRule);
                mergedQuery.setContactRestriction(new FrontEndRestriction(ctcRest));
            }
        }
        return mergedQuery;
    }

    private static Restriction mergeRestrictions(Restriction rest1, Restriction rest2) {
        Restriction merged = null;
        if (rest1 != null && rest2 != null) {
            merged = Restriction.builder().and(rest1, rest2).build();
        } else if (rest1 != null) {
            merged = rest1;
        } else if (rest2 != null) {
            merged = rest2;
        }
        if (merged != null) {
            return RestrictionOptimizer.optimize(merged);
        } else {
            return null;
        }
    }

}
