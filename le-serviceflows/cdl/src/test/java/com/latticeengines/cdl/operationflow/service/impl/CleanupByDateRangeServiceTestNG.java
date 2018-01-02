package com.latticeengines.cdl.operationflow.service.impl;

import java.util.Calendar;
import java.util.Date;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

public class CleanupByDateRangeServiceTestNG {

    @Test(groups = "unit")
    public void invoke() {
        CleanupByDateRangeService service = new CleanupByDateRangeService();
        DataCollectionProxy dataCollectionProxy = Mockito.mock(DataCollectionProxy.class);
        service.setDataCollectionProxy(dataCollectionProxy);

        CleanupByDateRangeConfiguration config = new CleanupByDateRangeConfiguration();
        try {
            service.invoke(config);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("CustomerSpace in CleanupByDateRangeConfig is null."));
        }

        config.setCustomerSpace("test_customerspace");
        try {
            service.invoke(config);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("Entity in CleanupByDateRangeConfig is not Transaction."));
        }

        config.setEntity(BusinessEntity.Transaction);
        try {
            service.invoke(config);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("StartTime or EndTime in CleanupByDateRangeConfig is null."));
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        config.setEndTime(calendar.getTime());

        calendar.add(Calendar.DAY_OF_YEAR, 2);
        config.setStartTime(calendar.getTime());
        try {
            service.invoke(config);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("StartTime cannot after EndTime in CleanupByDateRangeConfig."));
        }

        calendar.add(Calendar.DAY_OF_YEAR, -4);
        config.setStartTime(calendar.getTime());

        Mockito.when(dataCollectionProxy.getTable(config.getCustomerSpace(), TableRoleInCollection
                .ConsolidatedRawTransaction)).thenReturn(null);
        try {
            service.invoke(config);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("Table is null."));
        }

        Table table = new Table();
        Mockito.when(dataCollectionProxy.getTable(config.getCustomerSpace(), TableRoleInCollection
                .ConsolidatedRawTransaction)).thenReturn(table);
        try {
            service.invoke(config);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().startsWith("There are 0 or more than 1 extract in table."));
        }
    }
}
