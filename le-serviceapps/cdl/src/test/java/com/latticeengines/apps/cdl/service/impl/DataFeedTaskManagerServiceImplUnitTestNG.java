package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
public class DataFeedTaskManagerServiceImplUnitTestNG {

    private static final String illegalHeader = "bi0mpJJNxiYpEka5C6JO4mjopVGId/Tyrac80t5CKI/9bs74cuMaGOMSp8SBgBkE";
    private DataFeedTaskManagerServiceImpl dataFeedTaskManagerServiceImpl = new DataFeedTaskManagerServiceImpl(null,
            null, null, null, null, null, null, null, null, null);

    @Test(groups = "unit")
    public void testUpdateTableAttrName() {
        Table templateTable = new Table();
        Attribute attribute1 = new Attribute("AccountId");
        Attribute attribute2 = new Attribute("TestAttr");
        templateTable.addAttributes(Arrays.asList(attribute1, attribute2));
        Table metaTable = new Table();
        Attribute attribute3 = new Attribute("AccountId");
        Attribute attribute4 = new Attribute("testATTR");
        metaTable.addAttributes(Arrays.asList(attribute3, attribute4));
        Assert.assertNotNull(metaTable.getAttribute("testATTR"));
        Assert.assertNotNull(metaTable.getAttribute("AccountId"));
        Assert.assertNull(metaTable.getAttribute("TestAttr"));
        dataFeedTaskManagerServiceImpl.updateTableAttributeName(templateTable, metaTable);
        Assert.assertNull(metaTable.getAttribute("testATTR"));
        Assert.assertNotNull(metaTable.getAttribute("AccountId"));
        Assert.assertNotNull(metaTable.getAttribute("TestAttr"));
    }

    @Test(groups = "unit")
    public void testFinalSchemaCheck() {
        Table table1 = SchemaRepository.instance().getSchema(BusinessEntity.Account);
        Table table2 = SchemaRepository.instance().getSchema(BusinessEntity.Account);
        Attribute attribute2 = new Attribute("TestAttr");
        attribute2.setPhysicalDataType("String");
        table2.addAttribute(attribute2);
        Assert.assertTrue(dataFeedTaskManagerServiceImpl.finalSchemaCheck(table1, "Account", false));
        Assert.assertTrue(dataFeedTaskManagerServiceImpl.finalSchemaCheck(table2, "Account", false));
        table1.getAttribute(InterfaceName.AccountId).setPhysicalDataType("Int");
        Assert.assertFalse(dataFeedTaskManagerServiceImpl.finalSchemaCheck(table1, "Account", false));
        table2.getAttribute("TestAttr").setPhysicalDataType("Int");
        Assert.assertTrue(dataFeedTaskManagerServiceImpl.finalSchemaCheck(table2, "Account", false));

    }

    @Test(groups = "unit")
    public void testS3Validate() throws Exception {
        DataFeedTask dataFeedTask = new DataFeedTask();
        Table templateTable = new Table();
        Attribute attribute1 = new Attribute("AccountId");
        attribute1.setDisplayName("AccountId");
        attribute1.setRequired(true);
        Attribute attribute2 = new Attribute("TestAttr");
        attribute1.setDisplayName("TestAttr");
        dataFeedTask.setImportTemplate(templateTable);
        templateTable.addAttributes(Arrays.asList(attribute1, attribute2));
        S3FileToHdfsConfiguration importConfig = new S3FileToHdfsConfiguration();
        String customerSpace = "test";
        S3ImportEmailInfo emailInfo = new S3ImportEmailInfo();

        S3Service s3Service = Mockito.mock(S3Service.class);
        String headerOutof64 = "id," + illegalHeader + "\n";
        String headerwithCSVInjection = "id,@second\n";
        InputStream stream1 = new ByteArrayInputStream(headerOutof64.getBytes());
        InputStream stream2 = new ByteArrayInputStream(headerwithCSVInjection.getBytes());
        InputStream stream3 = new ByteArrayInputStream("id,TestAttr\n".getBytes());
        Mockito.when(s3Service.readObjectAsStream(anyString(), anyString())).thenReturn(stream1).thenReturn(stream2)
                .thenReturn(stream3);

        DataFeedTaskManagerServiceImpl dataFeedTaskManagerService = Mockito.mock(DataFeedTaskManagerServiceImpl.class);
        ReflectionTestUtils.setField(dataFeedTaskManagerService, "s3Service", s3Service);
        String initialS3FilePath = "dropfolder/vjcrdj9s/Templates/AccountData/tinatest012302.csv";
        try {
            dataFeedTaskManagerService.validateS3File(dataFeedTask, importConfig, customerSpace, emailInfo,
                    initialS3FilePath);
        } catch (Exception e) {
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18188);
        }
        try {
            dataFeedTaskManagerService.validateS3File(dataFeedTask, importConfig, customerSpace, emailInfo,
                    initialS3FilePath);
        } catch (Exception e) {
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18208);
        }
        try {
            dataFeedTaskManagerService.validateS3File(dataFeedTask, importConfig, customerSpace, emailInfo,
                    initialS3FilePath);
        } catch (Exception e) {
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_40043);
        }
    }

    @Test(groups = "unit")
    public void testUpdateAttrConfig() throws Exception {
        DataFeedTaskManagerServiceImpl dataFeedTaskManagerService = Mockito.mock(DataFeedTaskManagerServiceImpl.class);
        AttrConfigEntityMgr attrConfigEntityMgr = Mockito.mock(AttrConfigEntityMgr.class);
        List<AttrConfig> originalConfig = new ArrayList<>();
        Mockito.when(attrConfigEntityMgr.findAllForEntity(anyString(), any(BusinessEntity.class)))
                .thenReturn(originalConfig);
        Mockito.when(attrConfigEntityMgr.save(anyString(), any(BusinessEntity.class), anyList()))
                .thenReturn(originalConfig);
        ReflectionTestUtils.setField(dataFeedTaskManagerService, "attrConfigEntityMgr", attrConfigEntityMgr);
        Table templateTable = new Table();
        Attribute attribute1 = new Attribute("AccountId");
        attribute1.setSourceAttrName("AccountId");
        Attribute attribute2 = new Attribute("TestAttr");
        attribute1.setSourceAttrName("TestAttr");
        templateTable.addAttributes(Arrays.asList(attribute1, attribute2));
        List<AttrConfig> attrConfigs = new ArrayList<>();
        AttrConfig config1 = new AttrConfig();
        config1.setAttrName("testException");
        try {
            dataFeedTaskManagerService.updateAttrConfig(templateTable, attrConfigs, "Account",
                    CustomerSpace.parse("test"));
        } catch(Exception e) {
            Assert.assertEquals(true, e instanceof RuntimeException);
        }
    }
}
