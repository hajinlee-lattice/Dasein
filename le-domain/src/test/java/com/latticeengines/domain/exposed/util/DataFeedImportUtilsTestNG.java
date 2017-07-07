package com.latticeengines.domain.exposed.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Date;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask.Status;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class DataFeedImportUtilsTestNG {
    @Test(groups = "unit")
    public void testCreateImportFromTask() {
        DataFeed df = new DataFeed();
        df.setPid(1L);
        DataFeedTask task = new DataFeedTask();
        task.setPid(2L);
        task.setDataFeed(df);
        task.setActiveJob("Not specified");
        task.setEntity(SchemaInterpretation.Account.name());
        task.setSource("SFDC");
        task.setStatus(Status.Active);
        task.setSourceConfig("config");
        task.setImportTemplate(new Table(TableType.IMPORTTABLE));
        task.setImportData(null);
        task.setStartTime(new Date());
        task.setLastImported(new Date());
        task.setUniqueId(NamingUtils.uuid("DataFeedTask"));

        DataFeedImport dfImport = DataFeedImportUtils.createImportFromTask(task);
        assertEquals(dfImport.getEntity(), task.getEntity());
        assertEquals(dfImport.getDataTable(), task.getImportData());
        assertEquals(dfImport.getSource(), task.getSource());
        assertEquals(dfImport.getSourceConfig(), task.getSourceConfig());
        assertNull(dfImport.getPid());
        assertNull(dfImport.getExecution());
        assertNull(dfImport.getPid());

    }
}
