package com.latticeengines.objectapi.service.impl;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.util.QueryServiceUtils;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.factory.RedshiftQueryProvider;

@Component("transactionService")
public class TransactionServiceImpl implements TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionServiceImpl.class);

    @Inject
    private QueryEvaluatorService queryEvaluatorService;

    @Inject
    private PeriodProxy periodProxy;

    private LocalCacheManager<String, TimeFilterTranslator> timeTranslatorCache = null;

    @Override
    public String getMaxTransactionDate(DataCollection.Version version) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        return getMaxTransactionDate(customerSpace, version);
    }

    @Override
    public TimeFilterTranslator getTimeFilterTranslator(String evaluationDate) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        log.info("try to get timefilter for " + customerSpace + " with evaluation date " + evaluationDate);
        initializeTimeTranslatorCache();
        if (StringUtils.isBlank(evaluationDate)) {
            evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        }
        log.info("evaluationDate=" + evaluationDate);
        return timeTranslatorCache.getWatcherCache() //
                .get(String.format("%s|%s", customerSpace.getTenantId(), evaluationDate));
    }

    private TimeFilterTranslator getTimeFilterTranslatorBehindCache(String key) {
        String[] tokens = key.split("\\|");
        CustomerSpace customerSpace = CustomerSpace.parse(tokens[0]);
        String evaluationDate = tokens[1];
        return new TimeFilterTranslator(getPeriodStrategies(customerSpace), evaluationDate);
    }

    private String getMaxTransactionDate(CustomerSpace customerSpace, DataCollection.Version version) {
        AttributeLookup lookup = new AttributeLookup(BusinessEntity.Transaction, InterfaceName.TransactionDate.name());
        AggregateLookup lookup2 = AggregateLookup.max(lookup).as("max_txn_date");
        Query query = Query.builder() //
                .select(lookup2) //
                .from(BusinessEntity.Transaction) //
                .build();

        AttributeRepository attrRepo = QueryServiceUtils.checkAndGetAttrRepo(customerSpace, version,
                queryEvaluatorService);

        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(500);
        backOffPolicy.setMultiplier(2.0);
        retry.setBackOffPolicy(backOffPolicy);
        retry.setThrowLastExceptionOnExhausted(true);

        return retry.execute(context -> {
            DataPage dataPage = queryEvaluatorService.getData(attrRepo, query, RedshiftQueryProvider.USER_SEGMENT);
            return (String) dataPage.getData().get(0).get("max_txn_date");
        });
    }

    @Override
    public boolean needTimeFilterTranslator(FrontEndQuery frontEndQuery) {
        boolean hasTxn = false;
        if (frontEndQuery.getAccountRestriction() != null) {
            Restriction restriction = frontEndQuery.getAccountRestriction().getRestriction();
            hasTxn = hasTransactionOrDateFilterBucket(restriction);
        }
        if (frontEndQuery.getContactRestriction() != null) {
            Restriction restriction = frontEndQuery.getContactRestriction().getRestriction();
            hasTxn = hasTxn || hasTransactionOrDateFilterBucket(restriction);
        }
        if (CollectionUtils.isNotEmpty(frontEndQuery.getRatingModels())) {
            for (RatingModel ratingModel : frontEndQuery.getRatingModels()) {
                if (ratingModel instanceof RuleBasedModel) {
                    hasTxn = hasTxn || hasTransactionOrDateFilterBucket((RuleBasedModel) ratingModel);
                }
            }
        }
        log.info("frontendquery=" + frontEndQuery + ", and hasTxn=" + hasTxn);
        return hasTxn;
    }

    private boolean hasTransactionOrDateFilterBucket(RuleBasedModel model) {
        boolean hasTxn = false;
        if (model.getRatingRule() != null) {
            Stream<Restriction> restrictions = model.getRatingRule().getBucketToRuleMap().values().stream()
                    .flatMap(map -> map.values().stream()).filter(Objects::nonNull);
            hasTxn = restrictions.anyMatch(this::hasTransactionOrDateFilterBucket);
        }
        return hasTxn;
    }

    private boolean hasTransactionOrDateFilterBucket(Restriction restriction) {
        BreadthFirstSearch search = new BreadthFirstSearch();
        AtomicBoolean hasTxn = new AtomicBoolean(false);
        if (restriction != null) {
            search.run(restriction, (object, ctx) -> {
                if (object instanceof BucketRestriction) {
                    BucketRestriction bucket = (BucketRestriction) object;
                    if (!Boolean.TRUE.equals(bucket.getIgnored()) && isTransactionOrDateFilterBucket(bucket)) {
                        hasTxn.set(true);
                    }
                }
                if (object instanceof TransactionRestriction) {
                    hasTxn.set(true);
                }
            });
        }
        return hasTxn.get();
    }

    private boolean isTransactionOrDateFilterBucket(BucketRestriction restriction) {
        return restriction.getBkt().getTransaction() != null || restriction.getBkt().getDateFilter() != null;
    }

    private List<PeriodStrategy> getPeriodStrategies(CustomerSpace customerSpace) {
        return periodProxy.getPeriodStrategies(customerSpace.toString());
    }

    private synchronized void initializeTimeTranslatorCache() {
        if (timeTranslatorCache == null) {
            timeTranslatorCache = new LocalCacheManager<>( //
                    CacheName.TimeTranslatorCache, //
                    this::getTimeFilterTranslatorBehindCache, //
                    500); //
            timeTranslatorCache.getWatcherCache().setExpire(1, TimeUnit.DAYS);
            log.info("Initialized loading cache timeTranslatorCache.");
        }
    }

    @VisibleForTesting
    public void setPeriodProxy(PeriodProxy periodProxy) {
        this.periodProxy = periodProxy;
    }

}
