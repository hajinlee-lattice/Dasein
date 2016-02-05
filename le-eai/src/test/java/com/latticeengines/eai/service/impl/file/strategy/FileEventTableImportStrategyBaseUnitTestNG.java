package com.latticeengines.eai.service.impl.file.strategy;

import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.relique.jdbc.csv.CsvDriver;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;

public class FileEventTableImportStrategyBaseUnitTestNG {

    @Test(groups = "unit")
    public void createJdbcUrl() {
        Map<String, String> urlProperties = new HashMap<>();
        urlProperties.put(CsvDriver.DATE_FORMAT, "MM-DD-YYYY");
        urlProperties.put(CsvDriver.TIMESTAMP_FORMAT, CsvDriver.DEFAULT_TIME_FORMAT);
        ImportContext ctx = new ImportContext(null);
        ctx.setProperty(ImportProperty.FILEURLPROPERTIES, JsonUtils.serialize(urlProperties));

        String url = new FileEventTableImportStrategyBase().createJdbcUrl(ctx);
        assertTrue(url.equals("jdbc:relique:csv:./?timestampFormat=HH:mm:ss&dateFormat=MM-DD-YYYY")
                || url.equals("jdbc:relique:csv:./?dateFormat=MM-DD-YYYY&timestampFormat=HH:mm:ss"));
    }
}
