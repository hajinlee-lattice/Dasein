package com.latticeengines.app.exposed.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatchers.AMApiUpdate;

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

import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.exposed.service.EnrichmentService;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
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

    private WatcherCache<String, TopNAttributeTree> topAttrsCache;
    private Map<String, Boolean> flagMapForInternalEnrichment;

    @Autowired
    private AMStatsProxy amStatsProxy;

    @Autowired
    private AttributeService attributeService;

    @SuppressWarnings("unchecked")
    @PostConstruct
    public void postConstruct() {
        topAttrsCache = WatcherCache.builder(AMApiUpdate.name()) //
                .maximum(1) //
                .load(key -> {
                    if (DUMMY_KEY.equals(key)) {
                        TopNAttributeTree attributeTree = amStatsProxy.getTopAttrTree();
                        log.info("Loaded attributeTree into LoadingCache.");
                        List<LeadEnrichmentAttribute> allAttrs = attributeService.getAllAttributes();
                        Map<String, Boolean> updatedFlagMapForInternalEnrichment = new HashMap<>();
                        for (LeadEnrichmentAttribute attr : allAttrs) {
                            updatedFlagMapForInternalEnrichment.put(attr.getFieldName(), attr.getIsInternal());
                        }
                        flagMapForInternalEnrichment = updatedFlagMapForInternalEnrichment;
                        return attributeTree;
                    } else {
                        return null;
                    }
                }) //
                .initKeys(new String[] { DUMMY_KEY }) //
                .build();
        topAttrsCache.scheduleInit(30, TimeUnit.MINUTES);
    }

    @Override
    public void updateEnrichmentMatchFields(String id, List<MarketoMatchField> marketoMatchFields) {

    }

    @Override
    public AccountMasterCube getCube(AccountMasterFactQuery query) {
        AccountMasterCube cube = amStatsProxy.getCube(query, true);
        if (cube == null) {
            cube = new AccountMasterCube();
        }
        return cube;
    }

    @Override
    public AccountMasterCube getCube(String query) {
        AccountMasterFactQuery amQuery = parseAMFactQuery(query);
        return getCube(amQuery);
    }

    @Override
    public TopNAttributes getTopAttrs(Category category, int max, boolean shouldConsiderInternalEnrichment) {
        TopNAttributeTree topAttrsTree = getTopAttrTree();
        TopNAttributes topAttrsForCategory = topAttrsTree == null ? null : topAttrsTree.get(category);
        return selectTopN(topAttrsForCategory, max, shouldConsiderInternalEnrichment);
    }

    private TopNAttributeTree getTopAttrTree() {
        try {
            return topAttrsCache.get(DUMMY_KEY);
        } catch (Exception e) {
            log.error("Failed to load top attr tree from cache", e);
            TopNAttributeTree attributeTree = amStatsProxy.getTopAttrTree();
            return attributeTree;
        }
    }

    private TopNAttributes selectTopN(TopNAttributes attributes, int max, boolean shouldConsiderInternalEnrichment) {
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
                        if (!shouldConsiderInternalEnrichment
                                && flagMapForInternalEnrichment.get(attr.getAttribute()) != null
                                && flagMapForInternalEnrichment.get(attr.getAttribute()).booleanValue()) {
                            continue;
                        }
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

    private void loadCache() {
        try {
            topAttrsCache.get(DUMMY_KEY);
        } catch (Exception e) {
            log.error("Failed to load top attrs cache.", e);
        }
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
}
