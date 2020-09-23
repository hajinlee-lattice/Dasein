package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DanteConfigService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.dante.metadata.NotionMetadataWrapper;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

public class DanteConfigServiceImplFunctionalTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplDeploymentTestNG.class);

    private static final String commonResourcePath = "service/impl/dante-config/";
    private static final String accountMetadataPath = "AccountMetadata.json";

    @Inject
    private DanteConfigService danteConfigService;

    private DanteConfigurationDocument danteConfig;
    private ColumnMetadata testCm;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        ServingStoreProxy servingStoreProxy = mock(ServingStoreProxy.class);
        List<ColumnMetadata> list = getTestData(commonResourcePath + accountMetadataPath);
        testCm = list.get(100);
        doReturn((getTestData(commonResourcePath + accountMetadataPath))).when(servingStoreProxy).getAccountMetadata(
                any(String.class), any(ColumnSelection.Predefined.class), any(DataCollection.Version.class));
        ((DanteConfigServiceImpl) danteConfigService).setServingStoreProxy(servingStoreProxy);
    }

    @Test(groups = "functional")
    public void testGetDanteConfig() {
        danteConfig = danteConfigService.getDanteConfiguration();
        Assert.assertNotNull(danteConfig);
        Assert.assertNotNull(danteConfig.getMetadataDocument());
        Assert.assertTrue(danteConfig.getMetadataDocument().getNotions().stream()
                .filter(notion -> notion.getKey().equals("DanteAccount")).findFirst()
                .orElse(new NotionMetadataWrapper()).getValue().getProperties().stream()
                .anyMatch(x -> x.getName().equals(testCm.getAttrName())));
    }

    private List<ColumnMetadata> getTestData(String filePath) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader.getResourceAsStream(filePath);
        List<?> ret = JsonUtils.deserialize(dataStream, List.class);
        return JsonUtils.convertList(ret, ColumnMetadata.class);
    }
}
