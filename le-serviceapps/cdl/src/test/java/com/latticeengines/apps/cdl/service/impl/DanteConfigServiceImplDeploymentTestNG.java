package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DanteConfigService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

public class DanteConfigServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplDeploymentTestNG.class);

    private static final String commonResourcePath = "metadata/";
    private static final String accountMetadataPath = "AccountMetadata.json";

    @Inject
    private DanteConfigService danteConfigService;

    private DanteConfigurationDocument danteConfig;

    @BeforeClass(groups = "deployment")
    public void setup() {
        setupTestEnvironment();
        ServingStoreProxy servingStoreProxy = mock(ServingStoreProxy.class);
        doReturn((getTestData(commonResourcePath + accountMetadataPath))).when(servingStoreProxy).getAccountMetadata(
                any(String.class), any(ColumnSelection.Predefined.class), any(DataCollection.Version.class));
        ((DanteConfigServiceImpl) danteConfigService).setServingStoreProxy(servingStoreProxy);

    }

    @Test(groups = "deployment", enabled = true)
    public void testCrud() throws InterruptedException {
        danteConfig = danteConfigService.generateDanteConfig();

        List<DanteConfigurationDocument> danteConfigs = danteConfigService.findByTenant();
        Assert.assertTrue(CollectionUtils.isEmpty(danteConfigs));

        danteConfigService.createAndUpdateDanteConfig();
        Thread.sleep(500);
        danteConfigs = danteConfigService.findByTenant();
        DanteConfigurationDocument danteConfig1 = danteConfigService.getDanteConfigByTenantId();
        Assert.assertNotNull(danteConfig1);
        Assert.assertTrue(CollectionUtils.isNotEmpty(danteConfigs));
        Assert.assertEquals(danteConfigs.size(), 1);

        danteConfigService.deleteByTenant();
        Thread.sleep(100);
        danteConfigs = danteConfigService.findByTenant();
        Assert.assertTrue(CollectionUtils.isEmpty(danteConfigs));
        Assert.assertEquals(danteConfigs.size(), 0);
    }

    private List<ColumnMetadata> getTestData(String filePath) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream dataStream = classLoader.getResourceAsStream(filePath);
        List<?> ret = JsonUtils.deserialize(dataStream, List.class);
        List<ColumnMetadata> attrs = JsonUtils.convertList(ret, ColumnMetadata.class);
        return attrs;
    }

}
