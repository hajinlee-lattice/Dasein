package com.latticeengines.datacloud.match.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;

public class AccountMasterColumnServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Autowired
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    @Autowired
    private MetadataColumnEntityMgr<AccountMasterColumn> accountMasterColumnEntityMgr;

    private static final String DATA_CLOUD_VERSION_1 = "99.0.0";
    private static final String DATA_CLOUD_VERSION_2 = "99.0.1";
    private static final String DATA_CLOUD_VERSION_3 = "99.0.2";
    private static final AccountMasterColumn ACCOUNT_MASTER_COLUMN_1 = new AccountMasterColumn();
    private static final AccountMasterColumn ACCOUNT_MASTER_COLUMN_2 = new AccountMasterColumn();
    private static final AccountMasterColumn ACCOUNT_MASTER_COLUMN_3 = new AccountMasterColumn();
    private static final ColumnMetadata COLUMN_METADATA_1 = new ColumnMetadata();
    private static final ColumnMetadata COLUMN_METADATA_2 = new ColumnMetadata();
    private static final ColumnMetadata COLUMN_METADATA_3 = new ColumnMetadata();

    private static final String COLUMN_ID_1 = "COLUMN_ID_1";
    private static final String COLUMN_ID_2 = "COLUMN_ID_2";
    private static final String COLUMN_ID_3 = "COLUMN_ID_3";
    private static final String DISPLAY_NAME_1 = "Display Name 1";
    private static final String DISPLAY_NAME_2 = "Display Name 2";
    private static final String DISPLAY_NAME_3 = "Display Name 3";
    private static final String DESCRIPTION_1 = "Description 1";
    private static final String DESCRIPTION_2 = "Description 2";
    private static final String DESCRIPTION_3 = "Description 3";
    private static final String UPDATED_DESCRIPTION = "Updated Description";
    private static final String JAVA_STRING_CLASS = "String";
    private static final String JAVA_INTEGER_CLASS = "Integer";
    private static final String SUBCATEGORY_1 = "SubCategory_1";
    private static final String SUBCATEGORY_2 = "SubCategory_2";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        cleanupDB();

        ACCOUNT_MASTER_COLUMN_1.setAmColumnId(COLUMN_ID_1);
        ACCOUNT_MASTER_COLUMN_2.setAmColumnId(COLUMN_ID_2);
        ACCOUNT_MASTER_COLUMN_3.setAmColumnId(COLUMN_ID_3);
        ACCOUNT_MASTER_COLUMN_1.setDisplayName(DISPLAY_NAME_1);
        ACCOUNT_MASTER_COLUMN_2.setDisplayName(DISPLAY_NAME_2);
        ACCOUNT_MASTER_COLUMN_3.setDisplayName(DISPLAY_NAME_3);
        ACCOUNT_MASTER_COLUMN_1.setDescription(DESCRIPTION_1);
        ACCOUNT_MASTER_COLUMN_2.setDescription(DESCRIPTION_2);
        ACCOUNT_MASTER_COLUMN_3.setDescription(DESCRIPTION_3);
        ACCOUNT_MASTER_COLUMN_1.setJavaClass(JAVA_STRING_CLASS);
        ACCOUNT_MASTER_COLUMN_2.setJavaClass(JAVA_STRING_CLASS);
        ACCOUNT_MASTER_COLUMN_3.setJavaClass(JAVA_INTEGER_CLASS);
        ACCOUNT_MASTER_COLUMN_1.setSubcategory(SUBCATEGORY_1);
        ACCOUNT_MASTER_COLUMN_2.setSubcategory(SUBCATEGORY_1);
        ACCOUNT_MASTER_COLUMN_3.setSubcategory(SUBCATEGORY_2);
        ACCOUNT_MASTER_COLUMN_1.setDataCloudVersion(DATA_CLOUD_VERSION_1);
        ACCOUNT_MASTER_COLUMN_2.setDataCloudVersion(DATA_CLOUD_VERSION_2);
        ACCOUNT_MASTER_COLUMN_3.setDataCloudVersion(DATA_CLOUD_VERSION_3);
        ACCOUNT_MASTER_COLUMN_1.setCategory(Category.FIRMOGRAPHICS);
        ACCOUNT_MASTER_COLUMN_2.setCategory(Category.FIRMOGRAPHICS);
        ACCOUNT_MASTER_COLUMN_3.setCategory(Category.GROWTH_TRENDS);
        ACCOUNT_MASTER_COLUMN_1.setStatisticalType(StatisticalType.NOMINAL);
        ACCOUNT_MASTER_COLUMN_2.setStatisticalType(StatisticalType.NOMINAL);
        ACCOUNT_MASTER_COLUMN_3.setStatisticalType(StatisticalType.RATIO);
        ACCOUNT_MASTER_COLUMN_1.setFundamentalType(FundamentalType.ALPHA);
        ACCOUNT_MASTER_COLUMN_2.setFundamentalType(FundamentalType.ALPHA);
        ACCOUNT_MASTER_COLUMN_3.setFundamentalType(FundamentalType.NUMERIC);
        ACCOUNT_MASTER_COLUMN_1.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        ACCOUNT_MASTER_COLUMN_2.setApprovedUsage(ApprovedUsage.MODEL_MODELINSIGHTS);
        ACCOUNT_MASTER_COLUMN_3.setApprovedUsage(ApprovedUsage.MODEL);
        ACCOUNT_MASTER_COLUMN_1.setGroups("Enrichment");
        ACCOUNT_MASTER_COLUMN_2.setGroups("RTS,Enrichment");
        ACCOUNT_MASTER_COLUMN_3.setGroups("LeadEnrichment,Enrichment");

        List<DataCloudVersion> dataCloudVersions = prepareVersions();
        for (DataCloudVersion dataCloudVersion : dataCloudVersions) {
            dataCloudVersionEntityMgr.createVersion(dataCloudVersion);
        }
        accountMasterColumnEntityMgr.create(ACCOUNT_MASTER_COLUMN_1);
        accountMasterColumnEntityMgr.create(ACCOUNT_MASTER_COLUMN_2);
        accountMasterColumnEntityMgr.create(ACCOUNT_MASTER_COLUMN_3);
    }

    @Test(groups = "functional")
    public void updateMetadataColumnDescription_assertUpdated() {
        COLUMN_METADATA_1.setColumnId(COLUMN_ID_1);
        COLUMN_METADATA_1.setDisplayName(DISPLAY_NAME_1);
        COLUMN_METADATA_1.setDescription(UPDATED_DESCRIPTION);
        COLUMN_METADATA_1.setJavaClass(JAVA_STRING_CLASS);
        COLUMN_METADATA_1.setSubcategory(SUBCATEGORY_1);
        COLUMN_METADATA_1.setCategory(Category.FIRMOGRAPHICS);
        COLUMN_METADATA_1.setStatisticalType(StatisticalType.NOMINAL);
        COLUMN_METADATA_1.setFundamentalType(FundamentalType.ALPHA);
        COLUMN_METADATA_1.setCanBis(true);
        COLUMN_METADATA_1.setCanInsights(true);
        COLUMN_METADATA_1.setCanModel(true);
        COLUMN_METADATA_1.setCanEnrich(true);

        accountMasterColumnService.updateMetadataColumns(DATA_CLOUD_VERSION_1,
                Arrays.asList(COLUMN_METADATA_1));

        ACCOUNT_MASTER_COLUMN_1.setDescription(UPDATED_DESCRIPTION);
        assertTwoMetadataColumnsEqual(
                accountMasterColumnEntityMgr.findById(COLUMN_ID_1, DATA_CLOUD_VERSION_1),
                ACCOUNT_MASTER_COLUMN_1);
    }

    @Test(groups = "functional", dependsOnMethods = "updateMetadataColumnDescription_assertUpdated")
    public void updateAllMetadataColumns_assertAllUpdated() {
        COLUMN_METADATA_1.setColumnId(COLUMN_ID_1);
        COLUMN_METADATA_1.setDisplayName(DISPLAY_NAME_1);
        COLUMN_METADATA_1.setDescription(UPDATED_DESCRIPTION);
        COLUMN_METADATA_1.setJavaClass(JAVA_STRING_CLASS);
        COLUMN_METADATA_1.setSubcategory(SUBCATEGORY_1);
        COLUMN_METADATA_1.setCategory(Category.FIRMOGRAPHICS);
        COLUMN_METADATA_1.setStatisticalType(StatisticalType.NOMINAL);
        COLUMN_METADATA_1.setFundamentalType(FundamentalType.ALPHA);
        COLUMN_METADATA_1.setCanBis(false);
        COLUMN_METADATA_1.setCanInsights(true);
        COLUMN_METADATA_1.setCanModel(true);
        COLUMN_METADATA_1.setCanEnrich(true);

        COLUMN_METADATA_2.setColumnId(COLUMN_ID_2);
        COLUMN_METADATA_2.setDisplayName(DISPLAY_NAME_2);
        COLUMN_METADATA_2.setDescription(DESCRIPTION_2);
        COLUMN_METADATA_2.setJavaClass(JAVA_STRING_CLASS);
        COLUMN_METADATA_2.setSubcategory(SUBCATEGORY_1);
        COLUMN_METADATA_2.setCategory(Category.FIRMOGRAPHICS);
        COLUMN_METADATA_2.setStatisticalType(StatisticalType.NOMINAL);
        COLUMN_METADATA_2.setFundamentalType(FundamentalType.NUMERIC);
        COLUMN_METADATA_2.setCanBis(false);
        COLUMN_METADATA_2.setCanInsights(true);
        COLUMN_METADATA_2.setCanModel(true);
        COLUMN_METADATA_2.setCanEnrich(true);

        COLUMN_METADATA_3.setColumnId(COLUMN_ID_3);
        COLUMN_METADATA_3.setDisplayName(DISPLAY_NAME_3);
        COLUMN_METADATA_3.setDescription(DESCRIPTION_3);
        COLUMN_METADATA_3.setJavaClass(JAVA_INTEGER_CLASS);
        COLUMN_METADATA_3.setSubcategory(SUBCATEGORY_2);
        COLUMN_METADATA_3.setCategory(Category.GROWTH_TRENDS);
        COLUMN_METADATA_3.setStatisticalType(StatisticalType.RATIO);
        COLUMN_METADATA_3.setFundamentalType(FundamentalType.NUMERIC);
        COLUMN_METADATA_3.setCanBis(false);
        COLUMN_METADATA_3.setCanInsights(false);
        COLUMN_METADATA_3.setCanModel(true);
        COLUMN_METADATA_3.setCanEnrich(false);

        accountMasterColumnService.updateMetadataColumns(DATA_CLOUD_VERSION_1,
                Arrays.asList(COLUMN_METADATA_1));
        accountMasterColumnService.updateMetadataColumns(DATA_CLOUD_VERSION_2,
                Arrays.asList(COLUMN_METADATA_2));
        accountMasterColumnService.updateMetadataColumns(DATA_CLOUD_VERSION_3,
                Arrays.asList(COLUMN_METADATA_3));

        ACCOUNT_MASTER_COLUMN_1.setApprovedUsage(ApprovedUsage.MODEL_MODELINSIGHTS);
        assertTwoMetadataColumnsEqual(
                accountMasterColumnEntityMgr.findById(COLUMN_ID_1, DATA_CLOUD_VERSION_1),
                ACCOUNT_MASTER_COLUMN_1);
        ACCOUNT_MASTER_COLUMN_2.setFundamentalType(FundamentalType.NUMERIC);
        assertTwoMetadataColumnsEqual(
                accountMasterColumnEntityMgr.findById(COLUMN_ID_2, DATA_CLOUD_VERSION_2),
                ACCOUNT_MASTER_COLUMN_2);
        ACCOUNT_MASTER_COLUMN_3.setGroups("LeadEnrichment");
        assertTwoMetadataColumnsEqual(
                accountMasterColumnEntityMgr.findById(COLUMN_ID_3, DATA_CLOUD_VERSION_3),
                ACCOUNT_MASTER_COLUMN_3);
    }

    private void cleanupDB() {
        accountMasterColumnEntityMgr.deleteByColumnIdAndDataCloudVersion(COLUMN_ID_1,
                DATA_CLOUD_VERSION_1);
        accountMasterColumnEntityMgr.deleteByColumnIdAndDataCloudVersion(COLUMN_ID_2,
                DATA_CLOUD_VERSION_2);
        accountMasterColumnEntityMgr.deleteByColumnIdAndDataCloudVersion(COLUMN_ID_3,
                DATA_CLOUD_VERSION_3);

        dataCloudVersionEntityMgr.deleteVersion(DATA_CLOUD_VERSION_1);
        dataCloudVersionEntityMgr.deleteVersion(DATA_CLOUD_VERSION_2);
        dataCloudVersionEntityMgr.deleteVersion(DATA_CLOUD_VERSION_3);
    }

    private void assertTwoMetadataColumnsEqual(AccountMasterColumn actualColumn,
            AccountMasterColumn expectedColumn) {
        assertEquals(actualColumn.getAmColumnId(), expectedColumn.getAmColumnId());
        assertEquals(actualColumn.getDisplayName(), expectedColumn.getDisplayName());
        assertEquals(actualColumn.getDescription(), expectedColumn.getDescription());
        assertEquals(actualColumn.getJavaClass(), expectedColumn.getJavaClass());
        assertEquals(actualColumn.getSubcategory(), expectedColumn.getSubcategory());
        assertEquals(actualColumn.getCategory(), expectedColumn.getCategory());
        assertEquals(actualColumn.getStatisticalType(), expectedColumn.getStatisticalType());
        assertEquals(actualColumn.getFundamentalType(), expectedColumn.getFundamentalType());
        assertEquals(actualColumn.getApprovedUsage(), expectedColumn.getApprovedUsage());
        assertEquals(actualColumn.getGroups(), expectedColumn.getGroups());
        assertEquals(actualColumn.getDataCloudVersion(), expectedColumn.getDataCloudVersion());
        assertEquals(actualColumn.getDecodeStrategy(), expectedColumn.getDecodeStrategy());
        assertEquals(actualColumn.getDiscretizationStrategy(),
                expectedColumn.getDiscretizationStrategy());
        assertEquals(actualColumn.isInternalEnrichment(), expectedColumn.isInternalEnrichment());
        assertEquals(actualColumn.isPremium(), actualColumn.isPremium());
    }

    private List<DataCloudVersion> prepareVersions() {
        DataCloudVersion version1 = new DataCloudVersion();
        version1.setVersion(DATA_CLOUD_VERSION_1);
        version1.setCreateDate(new Date());
        version1.setAccountMasterHdfsVersion("version99");
        version1.setAccountLookupHdfsVersion("version99");
        version1.setMajorVersion("99.0");
        version1.setStatus(DataCloudVersion.Status.APPROVED);

        DataCloudVersion version2 = new DataCloudVersion();
        version2.setVersion(DATA_CLOUD_VERSION_2);
        version2.setCreateDate(new Date());
        version2.setAccountMasterHdfsVersion("version99");
        version2.setAccountLookupHdfsVersion("version99");
        version2.setMajorVersion("99.0");
        version2.setStatus(DataCloudVersion.Status.NEW);

        DataCloudVersion version3 = new DataCloudVersion();
        version3.setVersion(DATA_CLOUD_VERSION_3);
        version3.setCreateDate(new Date());
        version3.setAccountMasterHdfsVersion("version99");
        version3.setAccountLookupHdfsVersion("version99");
        version3.setMajorVersion("99.0");
        version3.setStatus(DataCloudVersion.Status.DEPRECATED);

        return Arrays.asList(version1, version2, version3);
    }

}
