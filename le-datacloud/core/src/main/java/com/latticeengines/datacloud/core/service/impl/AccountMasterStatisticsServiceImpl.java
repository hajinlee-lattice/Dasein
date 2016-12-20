package com.latticeengines.datacloud.core.service.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.metadata.Category;

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
                numEmpRangeId, revRangeId,
                numLocRangeId, categoryId);
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
        AccountMasterCube cube = new AccountMasterCube();
        cube.setStatistics(statistics.get(ACCOUNTMASTER_STATISTICS_KEY));
        return cube;
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

}
