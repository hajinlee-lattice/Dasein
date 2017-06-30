package com.latticeengines.datacloud.match.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public class AccountMasterColumnMetadataServiceImplTestNG
        extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Autowired
    private ColumnMetadataService accountMasterColumnMetadataService;

    @Autowired
    private MetadataColumnEntityMgr<AccountMasterColumn> accountMasterColumnEntityMgr;

    private static final String DATA_CLOUD_VERSION_1 = "99.0.0";
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
    private static final ColumnSelection COLUMN_SELECTION = new ColumnSelection();
    private static final Column COLUMN_1 = new Column();
    private static final Column COLUMN_2 = new Column();
    private static final Column COLUMN_3 = new Column();

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
        ACCOUNT_MASTER_COLUMN_2.setDataCloudVersion(DATA_CLOUD_VERSION_1);
        ACCOUNT_MASTER_COLUMN_3.setDataCloudVersion(DATA_CLOUD_VERSION_1);
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
    private void updateColumnMetadatas_assertCorrectUpdatesApplied() {
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

        accountMasterColumnMetadataService.updateColumnMetadatas(DATA_CLOUD_VERSION_1,
                Arrays.asList(COLUMN_METADATA_1, COLUMN_METADATA_2, COLUMN_METADATA_3));
        COLUMN_1.setExternalColumnId(COLUMN_ID_1);
        COLUMN_2.setExternalColumnId(COLUMN_ID_2);
        COLUMN_3.setExternalColumnId(COLUMN_ID_3);
        COLUMN_SELECTION.setColumns(Arrays.asList(COLUMN_1, COLUMN_2, COLUMN_3));
        List<ColumnMetadata> updatedColumnMetadatas = accountMasterColumnMetadataService
                .fromSelection(COLUMN_SELECTION, DATA_CLOUD_VERSION_1);
        COLUMN_METADATA_1.setApprovedUsageList(Arrays.asList(ApprovedUsage.MODEL_ALLINSIGHTS));
        COLUMN_METADATA_2.setApprovedUsageList(Arrays.asList(ApprovedUsage.MODEL_MODELINSIGHTS));
        COLUMN_METADATA_3.setApprovedUsageList(Arrays.asList(ApprovedUsage.MODEL));
        assertTwoColumnMetadatasEqual(updatedColumnMetadatas.get(0), COLUMN_METADATA_1);
        assertTwoColumnMetadatasEqual(updatedColumnMetadatas.get(1), COLUMN_METADATA_2);
        assertTwoColumnMetadatasEqual(updatedColumnMetadatas.get(2), COLUMN_METADATA_3);
    }

    @Test(groups = "functional", dependsOnMethods = "updateColumnMetadatas_assertCorrectUpdatesApplied")
    private void updateErroneousApprovedUsages_assertCorrectErrorThrown() {
        COLUMN_METADATA_1.setColumnId(COLUMN_ID_1);
        COLUMN_METADATA_1.setDisplayName(DISPLAY_NAME_1);
        COLUMN_METADATA_1.setDescription(UPDATED_DESCRIPTION);
        COLUMN_METADATA_1.setJavaClass(JAVA_STRING_CLASS);
        COLUMN_METADATA_1.setSubcategory(SUBCATEGORY_1);
        COLUMN_METADATA_1.setCategory(Category.FIRMOGRAPHICS);
        COLUMN_METADATA_1.setStatisticalType(StatisticalType.NOMINAL);
        COLUMN_METADATA_1.setFundamentalType(FundamentalType.ALPHA);
        COLUMN_METADATA_1.setCanBis(true);
        COLUMN_METADATA_1.setCanInsights(false);
        COLUMN_METADATA_1.setCanModel(true);
        COLUMN_METADATA_1.setCanEnrich(true);

        try {
            accountMasterColumnMetadataService.updateColumnMetadatas(DATA_CLOUD_VERSION_1,
                    Arrays.asList(COLUMN_METADATA_1));
            assertTrue(false, "should have thrown exception when approved usage conflicts");
        } catch (Exception e) {
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_25026);
        }
    }

    private void assertTwoColumnMetadatasEqual(ColumnMetadata actualColumn,
            ColumnMetadata expectedColumn) {
        assertEquals(actualColumn.getColumnId(), expectedColumn.getColumnId());
        assertEquals(actualColumn.getDescription(), expectedColumn.getDescription());
        assertEquals(actualColumn.getDisplayName(), expectedColumn.getDisplayName());
        assertEquals(actualColumn.getFundamentalType(), expectedColumn.getFundamentalType());
        assertEquals(actualColumn.getDiscretizationStrategy(),
                expectedColumn.getDiscretizationStrategy());
        assertEquals(actualColumn.getApprovedUsageString(),
                expectedColumn.getApprovedUsageString());
        assertEquals(actualColumn.getCategory(), expectedColumn.getCategory());
        assertEquals(actualColumn.getJavaClass(), expectedColumn.getJavaClass());
        assertEquals(new Boolean(Boolean.TRUE.equals(actualColumn.isCanEnrich())), expectedColumn.isCanEnrich());
        assertEquals(new Boolean(Boolean.TRUE.equals(actualColumn.isCanBis())), expectedColumn.isCanBis());
        assertEquals(new Boolean(Boolean.TRUE.equals(actualColumn.isCanInsights())), expectedColumn.isCanInsights());
        assertEquals(new Boolean(Boolean.TRUE.equals(actualColumn.isCanModel())), expectedColumn.isCanModel());
        assertEquals(actualColumn.getStatisticalType(), expectedColumn.getStatisticalType());
        assertEquals(actualColumn.getSubcategory(), expectedColumn.getSubcategory());
    }

    private void cleanupDB() {
        accountMasterColumnEntityMgr.deleteByColumnIdAndDataCloudVersion(COLUMN_ID_1,
                DATA_CLOUD_VERSION_1);
        accountMasterColumnEntityMgr.deleteByColumnIdAndDataCloudVersion(COLUMN_ID_2,
                DATA_CLOUD_VERSION_1);
        accountMasterColumnEntityMgr.deleteByColumnIdAndDataCloudVersion(COLUMN_ID_3,
                DATA_CLOUD_VERSION_1);

        dataCloudVersionEntityMgr.deleteVersion(DATA_CLOUD_VERSION_1);
    }

    private List<DataCloudVersion> prepareVersions() {
        DataCloudVersion version1 = new DataCloudVersion();
        version1.setVersion(DATA_CLOUD_VERSION_1);
        version1.setCreateDate(new Date());
        version1.setAccountMasterHdfsVersion("version99");
        version1.setAccountLookupHdfsVersion("version99");
        version1.setMajorVersion("99.0");
        version1.setStatus(DataCloudVersion.Status.APPROVED);
        version1.setMetadataRefreshDate(new Date());
        return Arrays.asList(version1);
    }
}
