package com.latticeengines.datacloud.core.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.datacloud.core.entitymgr.AccountMasterFactEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.AccountMasterStatisticsService;
import com.latticeengines.datacloud.core.service.DimensionalQueryService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes.TopAttribute;
import com.latticeengines.domain.exposed.metadata.Category;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("accountMasterStatisticsService")
public class AccountMasterStatisticsServiceImpl implements AccountMasterStatisticsService {

    private static final Log log = LogFactory.getLog(AccountMasterStatisticsServiceImpl.class);

    private static final String ACCOUNTMASTER_STATISTICS_KEY = "statistics";

    @Autowired
    private AccountMasterFactEntityMgr accountMasterFactEntityMgr;

    @Autowired
    private CategoricalAttributeEntityMgr categoricalAttributeEntityMgr;

    @Autowired
    private DimensionalQueryService dimensionalQueryService;

    @Override
    public AccountMasterCube query(AccountMasterFactQuery query) {
        Long locationId = dimensionalQueryService.findAttrId(query.getLocationQry());
        Long industryId = dimensionalQueryService.findAttrId(query.getIndustryQry());
        Long numEmpRangeId = dimensionalQueryService.findAttrId(query.getNumEmpRangeQry());
        Long revRangeId = dimensionalQueryService.findAttrId(query.getRevRangeQry());
        Long numLocRangeId = dimensionalQueryService.findAttrId(query.getNumLocRangeQry());
        Long categoryId = dimensionalQueryService.findAttrId(query.getCategoryQry());
        AccountMasterFact accountMasterFact = accountMasterFactEntityMgr.findByDimensions(locationId, industryId,
                numEmpRangeId, revRangeId, numLocRangeId, categoryId);
        if (accountMasterFact != null) {
            Map<String, AttributeStatistics> statistics = getAMStatisticsFromAMFact(accountMasterFact);
            AccountMasterCube cube = new AccountMasterCube();
            cube.setStatistics(statistics);
            return cube;
        } else {
            return null;
        }
    }

    @Override
    public TopNAttributeTree getTopAttrTree() {
        TopNAttributeTree topNAttributeTree = new TopNAttributeTree();
        AccountMasterFactQuery query = createQueryForTopAttrTree();
        Long locationId = dimensionalQueryService.findAttrId(query.getLocationQry());
        Long industryId = dimensionalQueryService.findAttrId(query.getIndustryQry());
        Long numEmpRangeId = dimensionalQueryService.findAttrId(query.getNumEmpRangeQry());
        Long revRangeId = dimensionalQueryService.findAttrId(query.getRevRangeQry());
        Long numLocRangeId = dimensionalQueryService.findAttrId(query.getNumLocRangeQry());
        Map<Category, Long> categories = getCategories();
        for (Category category : categories.keySet()) {
            TopNAttributes topNAttributes = new TopNAttributes();
            Map<String, Long> subCategories = getSubCategories(category);
            for (String subCategory : subCategories.keySet()) {
                AccountMasterFact accountMasterFact = accountMasterFactEntityMgr.findByDimensions(locationId,
                        industryId, numEmpRangeId, revRangeId, numLocRangeId, subCategories.get(subCategory));
                if (accountMasterFact != null) {
                    Map<String, AttributeStatistics> statistics = getAMStatisticsFromAMFact(accountMasterFact);
                    List<Entry<String, AttributeStatistics>> sortedStatistics = new ArrayList<Entry<String, AttributeStatistics>>(
                            statistics.entrySet());
                    Collections.sort(sortedStatistics, new Comparator<Entry<String, AttributeStatistics>>() {
                        // Descending order
                        public int compare(Entry<String, AttributeStatistics> s1,
                                Entry<String, AttributeStatistics> s2) {
                            if (s1.getValue().getRowBasedStatistics().getNonNullCount() > s2.getValue()
                                    .getRowBasedStatistics()
                                    .getNonNullCount()) {
                                return -1;
                            } else if (s1.getValue().getRowBasedStatistics().getNonNullCount() < s2.getValue()
                                    .getRowBasedStatistics()
                                    .getNonNullCount()) {
                                return 1;
                            } else {
                                return 0;
                            }
                        }
                    });
                    for (Entry<String, AttributeStatistics> statistic : sortedStatistics) {
                        TopAttribute topAttribute = new TopAttribute(statistic.getKey(),
                                statistic.getValue().getRowBasedStatistics().getNonNullCount());
                        topNAttributes.addTopAttribute(subCategory, topAttribute);
                    }
                }
            }
            topNAttributeTree.put(category, topNAttributes);
        }
        return topNAttributeTree;
    }

    @Override
    public Long getAttrId(String columnName, String categoricalValue) {
        CategoricalAttribute attribute = categoricalAttributeEntityMgr.getAttribute(columnName, categoricalValue);
        return attribute == null ? null : attribute.getPid();
    }

    @Override
    public Map<Category, Long> getCategories() {
        Map<Category, Long> catIdMap = new HashMap<>();
        for (Category category : Category.values()) {
            Long rootAttrId = getCategoryRootId(category);
            catIdMap.put(category, rootAttrId);
        }
        return catIdMap;
    }

    @Override
    public Map<String, Long> getSubCategories(Category category) {
        Map<String, Long> subCatIdMap = new HashMap<>();
        Long rootAttrId = getCategoryRootId(category);
        List<CategoricalAttribute> children = categoricalAttributeEntityMgr.getChildren(rootAttrId);
        for (CategoricalAttribute attribute : children) {
            subCatIdMap.put(attribute.getAttrValue(), attribute.getPid());
        }
        return subCatIdMap;
    }

    private Long getCategoryRootId(Category category) {
        DimensionalQuery query = new DimensionalQuery();
        query.setSource(DataCloudConstants.ACCOUNT_MASTER_COLUMN);
        query.setDimension(AccountMasterFact.DIM_CATEGORY);
        Map<String, String> qualifiers = new HashMap<>();
        qualifiers.put(DataCloudConstants.ATTR_CATEGORY, category.name());
        query.setQualifiers(qualifiers);
        return dimensionalQueryService.findAttrId(query);
    }

    private Map<String, AttributeStatistics> getAMStatisticsFromAMFact(AccountMasterFact accountMasterFact) {
        byte[] decodeCubeArr = Base64Utils.decodeBase64(accountMasterFact.getEncodedCube());
        String decodeCube = new String(decodeCubeArr);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<String, AttributeStatistics>> statistics = new HashMap<>();
        try {
            statistics = mapper.readValue(decodeCube,
                    new TypeReference<Map<String, Map<String, AttributeStatistics>>>() {
                    });
        } catch (IOException e) {
            throw new RuntimeException(String.format("Fail to parse json object of %s", decodeCube));
        }
        return statistics.get(ACCOUNTMASTER_STATISTICS_KEY);
    }

    private AccountMasterFactQuery createQueryForTopAttrTree() {
        AccountMasterFactQuery query = new AccountMasterFactQuery();
        DimensionalQuery locationQry = new DimensionalQuery();
        locationQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        locationQry.setDimension(DataCloudConstants.DIMENSION_LOCATION);
        Map<String, String> qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_COUNTRY, CategoricalAttribute.ALL);
        locationQry.setQualifiers(qualifiers);
        query.setLocationQry(locationQry);
        DimensionalQuery industryQry = new DimensionalQuery();
        industryQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        industryQry.setDimension(DataCloudConstants.DIMENSION_INDUSTRY);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_INDUSTRY, CategoricalAttribute.ALL);
        industryQry.setQualifiers(qualifiers);
        query.setIndustryQry(industryQry);
        DimensionalQuery numEmpRangeQry = new DimensionalQuery();
        numEmpRangeQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        numEmpRangeQry.setDimension(DataCloudConstants.DIMENSION_NUM_EMP_RANGE);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_NUM_EMP_RANGE, CategoricalAttribute.ALL);
        numEmpRangeQry.setQualifiers(qualifiers);
        query.setNumEmpRangeQry(numEmpRangeQry);
        DimensionalQuery revRangeQry = new DimensionalQuery();
        revRangeQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        revRangeQry.setDimension(DataCloudConstants.DIMENSION_REV_RANGE);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_REV_RANGE, CategoricalAttribute.ALL);
        revRangeQry.setQualifiers(qualifiers);
        query.setRevRangeQry(revRangeQry);
        DimensionalQuery numLocRangeQry = new DimensionalQuery();
        numLocRangeQry.setSource(DataCloudConstants.ACCOUNT_MASTER);
        numLocRangeQry.setDimension(DataCloudConstants.DIMENSION_NUM_LOC_RANGE);
        qualifiers = new HashMap<String, String>();
        qualifiers.put(DataCloudConstants.ATTR_NUM_LOC_RANGE, CategoricalAttribute.ALL);
        numLocRangeQry.setQualifiers(qualifiers);
        query.setNumLocRangeQry(numLocRangeQry);
        return query;
    }

}
