package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.anyString;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

public class RedShiftCleanupServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RedShiftCleanupServiceImplTestNG.class);

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private RedShiftCleanupService redshiftCleanupService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Value("${cdl.redshift.cleanup.table.remain.day}")
    private Long remainDay;

    private List<String> redshiftTables;
    private List<DataUnit> dataUnits;
    private List<String> metadataTables;
    private List<String> tenantNames;


    @BeforeClass(groups = "functional")
    public void setup() {
        redshiftTables = redshiftTablesProvider();
        dataUnits = dataUnitProvider();
        metadataTables = metadataTableProvider();
        tenantNames = tenantIdProvider();
        redshiftService = Mockito.mock(RedshiftService.class);
        tenantEntityMgr = Mockito.mock(TenantEntityMgr.class);
        dataCollectionService = Mockito.mock(DataCollectionService.class);
        dataUnitProxy = Mockito.mock(DataUnitProxy.class);
        Mockito.doNothing().when(redshiftService).renameTable(anyString(), anyString());
        Mockito.doNothing().when(redshiftService).dropTable(anyString());
        Mockito.when(redshiftService.getTables(anyString())).thenReturn(redshiftTables);
        Mockito.when(tenantEntityMgr.getAllTenantId()).thenReturn(tenantNames);
        Mockito.when(dataCollectionService.getAllTableNames()).thenReturn(metadataTables);
        Mockito.when(dataUnitProxy.getByNameAndType(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(DataUnit.StorageType.class))).thenAnswer((invocation) -> {
            String tenantName = invocation.getArgument(0);
            String name = invocation.getArgument(1);
            return getDataUnit(tenantName, name);});
        redshiftCleanupService = new RedShiftCleanupServiceImpl();
        // replace with the mock
        ReflectionTestUtils.setField(redshiftCleanupService, "redshiftService", redshiftService);
        ReflectionTestUtils.setField(redshiftCleanupService, "tenantEntityMgr", tenantEntityMgr);
        ReflectionTestUtils.setField(redshiftCleanupService, "dataCollectionService", dataCollectionService);
        ReflectionTestUtils.setField(redshiftCleanupService, "dataUnitProxy", dataUnitProxy);
        ReflectionTestUtils.setField(redshiftCleanupService, "cleanupFlag", true);
        ReflectionTestUtils.setField(redshiftCleanupService, "remainDay", remainDay);
    }

    @Test(groups = "functional")
    public void testCleanup() {
        redshiftCleanupService.removeUnusedTables();
    }

    private List<String> redshiftTablesProvider() {
        List<String> tableList = new ArrayList<>();
        String tableName = "";
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        tableList.add("tobedeletedon_20190121_atlas_qa_performance_d2_contact_2019_03_07_22_48_30_utc");
        String date = df.format(new Date());
        tableName = "tobedeletedon_" + date + "_atlas_qa_performance_dd_contact_2019_03_07_22_48_30_utc";
        tableList.add(tableName);
        df = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
        Calendar cal= Calendar.getInstance();
        cal.add(Calendar.DATE,(int)(1-remainDay));
        tableName = "atlas_qa_performance_dd_contact_" + df.format(cal.getTime()) + "_utc";
        tableList.add(tableName);
        tableList.add("tobedeletedon_20190121_bw_1113_autoimport_periodtransaction_2018_11_13_16_03_41_utc");
        tableList.add("bw_1113_autoimport_contact_2018_11_13_14_34_11_utc");
        tableList.add("bw_0907_cdl_depivotedpurchasehistory_2018_10_25_08_26_16_utc");
        tableList.add("auto_rulebasemodel_0307_periodtransaction_2018_03_07_11_37_21_utc");
        tableList.add("auto_rulebasemodel_0307_transaction_2018_03_07_11_37_21_utc");
        return tableList;
    }

    private List<DataUnit> dataUnitProvider() {
        List<DataUnit> dataUnits = new ArrayList<>();
        RedshiftDataUnit dataUnit = new RedshiftDataUnit();
        String tableName = "ToBedeletedOn_20190121_Bw_1113_AutoImport_PeriodTransaction_2018_11_13_16_03_41_UTC";
        dataUnit.setName(tableName);
        dataUnit.setRedshiftTable(tableName.toLowerCase());
        dataUnit.setTenant("Bw_1113_AutoImport");
        dataUnits.add(dataUnit);
        tableName = "Auto_RulebaseModel_0307_Transaction_2018_03_07_11_37_21_UTC";
        dataUnit.setTenant("Auto_RulebaseModel_0307");
        dataUnit.setName(tableName);
        dataUnit.setRedshiftTable(tableName.toLowerCase());
        dataUnits.add(dataUnit);
        return dataUnits;
    }

    private List<String> metadataTableProvider() {
        List<String> metadataTables = new ArrayList<>();
        metadataTables.add("BW_1113_AutoImport_Contact_2018_11_13_14_34_11_utc");
        return metadataTables;
    }

    private List<String> tenantIdProvider() {
        List<String> tenantIds = new ArrayList<>();
        tenantIds.add("BW_1113_autoimport.BW_1113_AutoImport.Production");
        tenantIds.add("Auto_RulebaseModel_0307.Auto_RulebaseModel_0307.Production");
        return tenantIds;
    }

    private DataUnit getDataUnit(String tenantName, String name) {
        for (DataUnit dataUnit: dataUnits) {
            if (dataUnit.getTenant() == tenantName && dataUnit.getName() == name)
                return dataUnit;
        }
        return null;
    }


}
