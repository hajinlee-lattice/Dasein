package com.latticeengines.matchapi.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AMStatsUtils;
import com.latticeengines.datacloud.core.entitymgr.AccountMasterFactEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DimensionalQueryService;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFactQuery;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributeTree;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes.TopAttribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.matchapi.service.AccountMasterStatisticsService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("accountMasterStatisticsService")
public class AccountMasterStatisticsServiceImpl implements AccountMasterStatisticsService {

    @Value("${datacloud.core.accountmasterstats.locationbased}")
    private boolean isLocationBased;

    @Autowired
    private AccountMasterFactEntityMgr accountMasterFactEntityMgr;

    @Autowired
    private CategoricalAttributeEntityMgr categoricalAttributeEntityMgr;

    @Autowired
    private DimensionalQueryService dimensionalQueryService;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    @Qualifier("accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Autowired
    @Qualifier("accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> columnEntityMgr;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Override
    public AccountMasterCube query(AccountMasterFactQuery query) {
        if (query.getLocationQry() == null) {
            query.setLocationQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER, AccountMasterFact.DIM_LOCATION,
                    DataCloudConstants.ATTR_COUNTRY));
        }
        Long locationId = dimensionalQueryService.findAttrId(query.getLocationQry());
        if (query.getIndustryQry() == null) {
            query.setIndustryQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER, AccountMasterFact.DIM_INDUSTRY,
                    DataCloudConstants.ATTR_INDUSTRY));
        }
        Long industryId = dimensionalQueryService.findAttrId(query.getIndustryQry());
        if (query.getNumEmpRangeQry() == null) {
            query.setNumEmpRangeQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER,
                    AccountMasterFact.DIM_NUM_EMP_RANGE, DataCloudConstants.ATTR_NUM_EMP_RANGE));
        }
        Long numEmpRangeId = dimensionalQueryService.findAttrId(query.getNumEmpRangeQry());
        if (query.getRevRangeQry() == null) {
            query.setRevRangeQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER, AccountMasterFact.DIM_REV_RANGE,
                    DataCloudConstants.ATTR_REV_RANGE));
        }
        Long revRangeId = dimensionalQueryService.findAttrId(query.getRevRangeQry());
        if (query.getNumLocRangeQry() == null) {
            query.setNumLocRangeQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER,
                    AccountMasterFact.DIM_NUM_LOC_RANGE, DataCloudConstants.ATTR_NUM_LOC_RANGE));
        }
        Long numLocRangeId = dimensionalQueryService.findAttrId(query.getNumLocRangeQry());
        if (query.getCategoryQry() == null) {
            query.setCategoryQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER_COLUMN,
                    AccountMasterFact.DIM_CATEGORY, DataCloudConstants.ATTR_CATEGORY));
        }
        Long categoryId = dimensionalQueryService.findAttrId(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER_COLUMN,
                AccountMasterFact.DIM_CATEGORY, DataCloudConstants.ATTR_CATEGORY));
        AccountMasterFact accountMasterFact = accountMasterFactEntityMgr.findByDimensions(locationId, industryId,
                numEmpRangeId, revRangeId, numLocRangeId, categoryId);
        if (accountMasterFact == null) {
            return null;
        }
        try {
            AccountMasterCube cube = AMStatsUtils.decompressAndDecode(accountMasterFact.getEncodedCube(),
                    AccountMasterCube.class);
            expandEncodedAttributes(cube);
            populateDummyBuckets(cube);
            cube = filterAttributes(cube, query.getCategoryQry().getQualifiers().get(DataCloudConstants.ATTR_CATEGORY),
                    query.getCategoryQry().getQualifiers().get(DataCloudConstants.ATTR_SUB_CATEGORY));
            return cube;
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Fail to parse json object %s", accountMasterFact.getEncodedCube()));
        }
    }

    @Override
    public TopNAttributeTree getTopAttrTree() {
        AccountMasterFactQuery query = createQueryForTopAttrTree();
        Long locationId = dimensionalQueryService.findAttrId(query.getLocationQry());
        Long industryId = dimensionalQueryService.findAttrId(query.getIndustryQry());
        Long numEmpRangeId = dimensionalQueryService.findAttrId(query.getNumEmpRangeQry());
        Long revRangeId = dimensionalQueryService.findAttrId(query.getRevRangeQry());
        Long numLocRangeId = dimensionalQueryService.findAttrId(query.getNumLocRangeQry());
        Long categoryId = dimensionalQueryService.findAttrId(query.getCategoryQry());
        AccountMasterFact accountMasterFact = accountMasterFactEntityMgr.findByDimensions(locationId, industryId,
                numEmpRangeId, revRangeId, numLocRangeId, categoryId);
        if (accountMasterFact == null) {
            return null;
        }
        AccountMasterCube cube;
        try {
            cube = AMStatsUtils.decompressAndDecode(accountMasterFact.getEncodedCube(), AccountMasterCube.class);
            expandEncodedAttributes(cube);
            cube = filterAttributes(cube, null, null);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Fail to parse json object %s", accountMasterFact.getEncodedCube()));
        }
        return createTopAttrTree(cube);
    }

    private TopNAttributeTree createTopAttrTree(AccountMasterCube cube) {
        Map<String, ColumnMetadata> columnsMetadata = getColumnMetadata();
        Map<String, AttributeStatistics> statistics = cube.getStatistics();
        List<Entry<String, AttributeStatistics>> sortedStatistics = new ArrayList<Entry<String, AttributeStatistics>>(
                statistics.entrySet());
        Collections.sort(sortedStatistics, new Comparator<Entry<String, AttributeStatistics>>() {
            // Descending order
            public int compare(Entry<String, AttributeStatistics> s1, Entry<String, AttributeStatistics> s2) {
                long valueS1, valueS2;
                if (isLocationBased) {
                    valueS1 = s1.getValue().getUniqueLocationBasedStatistics().getNonNullCount();
                    valueS2 = s2.getValue().getUniqueLocationBasedStatistics().getNonNullCount();
                } else {
                    valueS1 = s1.getValue().getRowBasedStatistics().getNonNullCount();
                    valueS2 = s2.getValue().getRowBasedStatistics().getNonNullCount();
                }
                if (valueS1 > valueS2) {
                    return -1;
                } else if (valueS1 < valueS2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });
        TopNAttributeTree tree = new TopNAttributeTree();
        for (Entry<String, AttributeStatistics> statistic : sortedStatistics) {
            Category category = columnsMetadata.get(statistic.getKey()).getCategory();
            String subCategory = columnsMetadata.get(statistic.getKey()).getSubcategory();
            TopNAttributes topNAttributes = tree.get(category);
            if (topNAttributes == null) {
                topNAttributes = new TopNAttributes();
            }
            if (isLocationBased) {
                topNAttributes.addTopAttribute(subCategory, new TopAttribute(statistic.getKey(),
                        statistic.getValue().getUniqueLocationBasedStatistics().getNonNullCount()));
            } else {
                topNAttributes.addTopAttribute(subCategory, new TopAttribute(statistic.getKey(),
                        statistic.getValue().getRowBasedStatistics().getNonNullCount()));
            }
            tree.put(category, topNAttributes);
        }
        return tree;
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

    // UI only shows attributes for enrichment
    private AccountMasterCube filterAttributes(AccountMasterCube cube, String category, String subCategory) {
        Map<String, ColumnMetadata> columnsMetadata = getColumnMetadata();
        Map<String, AttributeStatistics> statistics = cube.getStatistics();
        Iterator<Entry<String, AttributeStatistics>> iter = statistics.entrySet().iterator();
        long nonNullCount = 0;
        while (iter.hasNext()) {
            Entry<String, AttributeStatistics> entry = iter.next();
            if ((!columnsMetadata.containsKey(entry.getKey()))
                    || (category != null && !category.equals(CategoricalAttribute.ALL)
                            && !columnsMetadata.get(entry.getKey()).getCategory().name().equals(category))
                    || (subCategory != null && !subCategory.equals(CategoricalAttribute.ALL)
                            && !columnsMetadata.get(entry.getKey()).getSubcategory().equals(subCategory))) {
                iter.remove();
            }
            if (isLocationBased
                    && entry.getValue().getUniqueLocationBasedStatistics().getNonNullCount() > nonNullCount) {
                nonNullCount = entry.getValue().getUniqueLocationBasedStatistics().getNonNullCount();
            }
            if (!isLocationBased && entry.getValue().getRowBasedStatistics().getNonNullCount() > nonNullCount) {
                nonNullCount = entry.getValue().getRowBasedStatistics().getNonNullCount();
            }
        }
        cube.setNonNullCount(nonNullCount);
        return cube;
    }

    private Map<String, ColumnMetadata> getColumnMetadata() {
        String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        List<ColumnMetadata> allColumns = columnMetadataProxy.columnSelection(Predefined.Enrichment,
                currentDataCloudVersion);
        Map<String, ColumnMetadata> columnsMetadata = new HashMap<String, ColumnMetadata>();
        for (ColumnMetadata columnMetadata : allColumns) {
            columnsMetadata.put(columnMetadata.getColumnName(), columnMetadata);
        }
        return columnsMetadata;
    }

    private AccountMasterFactQuery createQueryForTopAttrTree() {
        AccountMasterFactQuery query = new AccountMasterFactQuery();
        query.setLocationQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER, AccountMasterFact.DIM_LOCATION,
                DataCloudConstants.ATTR_COUNTRY));
        query.setIndustryQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER, AccountMasterFact.DIM_INDUSTRY,
                DataCloudConstants.ATTR_INDUSTRY));
        query.setNumEmpRangeQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER,
                AccountMasterFact.DIM_NUM_EMP_RANGE, DataCloudConstants.ATTR_NUM_EMP_RANGE));
        query.setRevRangeQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER, AccountMasterFact.DIM_REV_RANGE,
                DataCloudConstants.ATTR_REV_RANGE));
        query.setNumLocRangeQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER,
                AccountMasterFact.DIM_NUM_LOC_RANGE, DataCloudConstants.ATTR_NUM_LOC_RANGE));
        query.setCategoryQry(createQueryForAll(DataCloudConstants.ACCOUNT_MASTER_COLUMN, AccountMasterFact.DIM_CATEGORY,
                DataCloudConstants.ATTR_CATEGORY));
        return query;
    }

    private DimensionalQuery createQueryForAll(String source, String dimension, String attribute) {
        DimensionalQuery query = new DimensionalQuery();
        query.setSource(source);
        query.setDimension(dimension);
        Map<String, String> qualifiers = new HashMap<String, String>();
        qualifiers.put(attribute, CategoricalAttribute.ALL);
        query.setQualifiers(qualifiers);
        return query;
    }

    private void expandEncodedAttributes(AccountMasterCube cube) {
        String dataCloudVersion = versionEntityMgr.latestApprovedForMajorVersion("2.0").getVersion();

        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columnList = new ArrayList<>();

        for (String columnName : cube.getStatistics().keySet()) {
            columnList.add(new Column(columnName));
        }
        columnSelection.setColumns(columnList);

        Map<String, List<String>> codeBookInfo = columnSelectionService.getEncodedColumnMapping(columnSelection,
                dataCloudVersion);

        for (String encodedColumnName : codeBookInfo.keySet()) {
            if (cube.getStatistics().containsKey(encodedColumnName)) {
                AttributeStatistics encodedValueCountInfo = cube.getStatistics().get(encodedColumnName);

                for (String attrName : codeBookInfo.get(encodedColumnName)) {
                    cube.getStatistics().put(attrName, encodedValueCountInfo);
                }
            }
        }
    }

    private void populateDummyBuckets(AccountMasterCube cube) {
        for (String columnName : cube.getStatistics().keySet()) {
            AttributeStatistics encodedValueCountInfo = cube.getStatistics().get(columnName);
            if (encodedValueCountInfo.getRowBasedStatistics() != null
                    && (encodedValueCountInfo.getRowBasedStatistics().getBuckets() == null //
                            || CollectionUtils.isEmpty(
                                    encodedValueCountInfo.getRowBasedStatistics().getBuckets().getBucketList()))) {

                if (encodedValueCountInfo.getRowBasedStatistics().getNonNullCount() > 0) {
                    Buckets buckets = new Buckets();
                    List<Bucket> bucketList = new ArrayList<>();
                    Bucket bucket = new Bucket();
                    bucket.setBucketLabel(columnName + " label A");
                    bucket.setCount(encodedValueCountInfo.getRowBasedStatistics().getNonNullCount() * 2 / 3);
                    bucketList.add(bucket);
                    bucket = new Bucket();
                    bucket.setBucketLabel(columnName + " label B");
                    bucket.setCount(encodedValueCountInfo.getRowBasedStatistics().getNonNullCount()
                            - encodedValueCountInfo.getRowBasedStatistics().getNonNullCount() * 2 / 3);
                    bucketList.add(bucket);
                    buckets.setBucketList(bucketList);
                    encodedValueCountInfo.getRowBasedStatistics().setBuckets(buckets);
                }
            }
        }
    }

}
