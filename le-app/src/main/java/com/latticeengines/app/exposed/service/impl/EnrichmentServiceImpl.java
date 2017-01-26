package com.latticeengines.app.exposed.service.impl;

import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.app.exposed.service.AttributeService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.proxy.exposed.matchapi.AMStatsProxy;

@Component("enrichmentService")
public class EnrichmentServiceImpl implements EnrichmentService {

    private static final Log log = LogFactory.getLog(EnrichmentServiceImpl.class);
    private static final String DUMMY_KEY = "TopNAttrTree";

    private boolean useDummyData = false;

    private LoadingCache<String, TopNAttributeTree> topAttrsCache;

    @Autowired
    private AMStatsProxy amStatsProxy;

    @Autowired
    private AttributeService attributeService;

    @PostConstruct
    public void postConstruct() {
        topAttrsCache = CacheBuilder.newBuilder().maximumSize(5).expireAfterWrite(1, TimeUnit.HOURS)
                .build(new CacheLoader<String, TopNAttributeTree>() {
                    @Override
                    public TopNAttributeTree load(String dummyKey) throws Exception {
                        TopNAttributeTree attributeTree;
                        if (useDummyData) {
                            attributeTree = new TopNAttributeTree();
                            for (Category category : Category.values()) {
                                attributeTree.put(category, createTopNAttributes(category.getName(), 20));
                            }
                        } else {
                            attributeTree = amStatsProxy.getTopAttrTree();
                        }
                        log.info("Loaded attributeTree into LoadingCache.");
                        return attributeTree;
                    }
                });
    }

    @Override
    public void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields) {

    }

    @Override
    public AccountMasterCube getCube(AccountMasterFactQuery query) {
        if (useDummyData) {
            return createDummyCube();
        } else {
            return amStatsProxy.getCube(query);
        }
    }

    @Override
    public AccountMasterCube getCube(String query) {
        AccountMasterFactQuery amQuery = parseAMFactQuery(query);
        return getCube(amQuery);
    }

    @Override
    public TopNAttributes getTopAttrs(Category category, int max) {
        TopNAttributeTree topAttrsTree = getTopAttrTree();
        TopNAttributes topAttrsForCategory = //
                topAttrsTree == null ? //
                        null : topAttrsTree.get(category);
        return selectTopN(topAttrsForCategory, max);
    }

    private TopNAttributeTree getTopAttrTree() {
        try {
            return topAttrsCache.get(DUMMY_KEY);
        } catch (Exception e) {
            log.error("Failed to load top attr tree from cache", e);
            TopNAttributeTree attributeTree;
            if (useDummyData) {
                attributeTree = new TopNAttributeTree();
                for (Category category : Category.values()) {
                    attributeTree.put(category, createTopNAttributes(category.getName(), 5));
                }
            } else {
                attributeTree = amStatsProxy.getTopAttrTree();
            }
            return attributeTree;
        }
    }

    private TopNAttributes selectTopN(TopNAttributes attributes, int max) {
        Map<String, List<TopNAttributes.TopAttribute>> topAttrs = new HashMap<>();
        TopNAttributes topNAttributes = new TopNAttributes();
        topNAttributes.setTopAttributes(topAttrs);

        if (attributes != null && !MapUtils.isEmpty(attributes.getTopAttributes())) {
            for (Map.Entry<String, List<TopNAttributes.TopAttribute>> entry : attributes.getTopAttributes()
                    .entrySet()) {
                List<TopNAttributes.TopAttribute> attrs = new ArrayList<>();
                String subCategory = entry.getKey();
                for (TopNAttributes.TopAttribute attr : entry.getValue()) {
                    if (attrs.size() < max) {
                        attrs.add(attr);
                    } else {
                        break;
                    }
                }
                topAttrs.put(subCategory, attrs);
            }
        }
        return topNAttributes;
    }

    private AccountMasterFactQuery parseAMFactQuery(String strQuery) {
        // for now, always return the biggest top cube
        return getAllTopAMFactQuery();
    }

    private AccountMasterFactQuery getAllTopAMFactQuery() {
        AccountMasterFactQuery query = new AccountMasterFactQuery();
        query.setCategoryQry(getCategoryTopQuery());
        query.setLocationQry(getLocationTopQuery());
        query.setIndustryQry(getIndustryTopQuery());
        query.setNumEmpRangeQry(getNumEmpRangeTopQuery());
        query.setRevRangeQry(getRevRangeTopoQuery());
        query.setNumLocRangeQry(getNumLocRangeTopQuery());
        return query;
    }

    private DimensionalQuery getCategoryTopQuery() {
        return getTopQuery(DataCloudConstants.ACCOUNT_MASTER_COLUMN, //
                AccountMasterFact.DIM_CATEGORY, //
                DataCloudConstants.ATTR_CATEGORY);
    }

    private DimensionalQuery getLocationTopQuery() {
        return getTopQuery(DataCloudConstants.ACCOUNT_MASTER, //
                AccountMasterFact.DIM_LOCATION, //
                DataCloudConstants.ATTR_COUNTRY);
    }

    private DimensionalQuery getIndustryTopQuery() {
        return getTopQuery(DataCloudConstants.ACCOUNT_MASTER, //
                AccountMasterFact.DIM_INDUSTRY, //
                DataCloudConstants.ATTR_INDUSTRY);
    }

    private DimensionalQuery getNumEmpRangeTopQuery() {
        return getTopQuery(DataCloudConstants.ACCOUNT_MASTER, //
                AccountMasterFact.DIM_NUM_EMP_RANGE, //
                DataCloudConstants.ATTR_NUM_EMP_RANGE);
    }

    private DimensionalQuery getRevRangeTopoQuery() {
        return getTopQuery(DataCloudConstants.ACCOUNT_MASTER, //
                AccountMasterFact.DIM_REV_RANGE, //
                DataCloudConstants.ATTR_REV_RANGE);
    }

    private DimensionalQuery getNumLocRangeTopQuery() {
        return getTopQuery(DataCloudConstants.ACCOUNT_MASTER, //
                AccountMasterFact.DIM_NUM_LOC_RANGE, //
                DataCloudConstants.ATTR_NUM_LOC_RANGE);
    }

    // TODO: should get root attribute name by calling some matchapi
    private DimensionalQuery getTopQuery(String source, String dimension, String rootAttr) {
        DimensionalQuery query = new DimensionalQuery();
        query.setSource(source);
        query.setDimension(dimension);
        Map<String, String> qualifiers = new HashMap<>();
        qualifiers.put(rootAttr, CategoricalAttribute.ALL);
        query.setQualifiers(qualifiers);
        return query;
    }

    private AccountMasterCube createDummyCube() {
        List<LeadEnrichmentAttribute> allAttrs = attributeService.getAllAttributes();
        return createDummyCube(allAttrs);
    }

    private AccountMasterCube createDummyCube(List<LeadEnrichmentAttribute> allAttrs) {
        AccountMasterCube cube = new AccountMasterCube();

        Map<String, AttributeStatistics> statistics = new HashMap<>();

        int count = 500000;
        for (LeadEnrichmentAttribute attr : allAttrs) {
            AttributeStatistics value = new AttributeStatistics();
            AttributeStatsDetails rowBasedStatistics = new AttributeStatsDetails();
            rowBasedStatistics.setNonNullCount(count--);
            Buckets buckets = new Buckets();
            buckets.setType(BucketType.Numerical);
            List<Bucket> bucketList = new ArrayList<>();
            Bucket bucket = new Bucket();
            bucket.setBucketLabel("First Bucket");
            bucket.setCount(count - 10);
            bucketList.add(bucket);
            bucket = new Bucket();
            bucket.setBucketLabel("Second Bucket");
            bucket.setCount(10);
            bucketList.add(bucket);
            buckets.setBucketList(bucketList);
            rowBasedStatistics.setBuckets(buckets);
            value.setRowBasedStatistics(rowBasedStatistics);

            statistics.put(attr.getFieldName(), value);
        }

        cube.setStatistics(statistics);
        return cube;
    }

    private TopNAttributes createTopNAttributes(String category, int max) {
        List<LeadEnrichmentAttribute> allAttrs = attributeService.getAllAttributes();
        TopNAttributes topNAttributes = new TopNAttributes();
        AccountMasterCube cube = createDummyCube(allAttrs);
        Map<String, List<TopNAttributes.TopAttribute>> topAttributes = new HashMap<>();
        topNAttributes.setTopAttributes(topAttributes);

        for (LeadEnrichmentAttribute attr : allAttrs) {
            if (!attr.getCategory().equalsIgnoreCase(category)) {
                continue;
            }

            String subcategory = attr.getSubcategory();
            if (!topAttributes.containsKey(subcategory)) {
                List<TopNAttributes.TopAttribute> topNList = new ArrayList<>();
                topAttributes.put(subcategory, topNList);
            }
            List<TopNAttributes.TopAttribute> topNList = topAttributes.get(subcategory);

            updateTopNList(attr, max, topNList, cube.getStatistics().get(attr.getFieldName()).getRowBasedStatistics());

        }

        return topNAttributes;
    }

    private void updateTopNList(LeadEnrichmentAttribute attr, int max, List<TopNAttributes.TopAttribute> topNList,
            AttributeStatsDetails rowBasedStatistics) {
        if (topNList.size() >= max) {
            return;
        } else {
            TopNAttributes.TopAttribute topAttr = new TopNAttributes.TopAttribute(attr.getFieldName(),
                    rowBasedStatistics.getNonNullCount());
            topNList.add(topAttr);
        }
    }

    private TopNAttributeTree constructDummyTree() {
        TopNAttributeTree tree = new TopNAttributeTree();
        return tree;
    }

}
