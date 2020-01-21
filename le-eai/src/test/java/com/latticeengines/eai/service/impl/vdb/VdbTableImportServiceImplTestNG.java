package com.latticeengines.eai.service.impl.vdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportVdbProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.functionalframework.VdbExtractAndImportUtil;
import com.latticeengines.eai.service.ImportService;
public class VdbTableImportServiceImplTestNG extends EaiFunctionalTestNGBase {
    @Inject
    private ImportService vdbTableImportService;
    @Inject
    private Configuration yarnConfiguration;

    private SourceImportConfiguration importConfig = new SourceImportConfiguration();

    private ImportContext importContext;

    @SuppressWarnings({ "rawtypes" })
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        List<Table> tables = new ArrayList<Table>();
        tables.add(VdbExtractAndImportUtil.createVdbActivity());
        importContext = new ImportContext(yarnConfiguration);
        importConfig.setTables(tables);
        importContext.setProperty(ImportVdbProperty.REPORT_STATUS_ENDPOINT, "http://localhost:8080");
        importContext.setProperty(ImportVdbProperty.VDB_QUERY_HANDLE, "vdb_query_handle");
        importContext.setProperty(ImportVdbProperty.TARGETPATH, "");
        importContext.setProperty(ImportVdbProperty.HADOOPCONFIG, yarnConfiguration);
        importContext.setProperty(ImportVdbProperty.EXTRACT_PATH, new HashMap());
        importContext.setProperty(ImportVdbProperty.PROCESSED_RECORDS, new HashMap());

        VdbSpecMetadata obj = new VdbSpecMetadata();
        obj.setColumnName("id");
        obj.setDisplayName("Id");
        obj.setDataType("Int");
        obj.setKeyColumn(true);

        VdbSpecMetadata obj2 = new VdbSpecMetadata();
        obj2.setColumnName("date");
        obj2.setDisplayName("Date");
        obj2.setDataType("Date");
        obj2.setKeyColumn(false);
        List<VdbSpecMetadata> array = new ArrayList<VdbSpecMetadata>();
        array.add(obj);
        array.add(obj2);
        importContext.setProperty(ImportVdbProperty.METADATA_LIST, JsonUtils.serialize(array));
    }

    @Test(groups = "functional", enabled = false)
    public void importMetadata() {
        List<Table> list = vdbTableImportService.importMetadata(importConfig, importContext, null);
        String[] attrs = list.get(0).getAttributeNames();
        Assert.assertEquals(attrs.length , 2);
    }


}
