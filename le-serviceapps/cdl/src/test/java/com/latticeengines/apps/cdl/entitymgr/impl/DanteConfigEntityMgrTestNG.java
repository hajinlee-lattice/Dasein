package com.latticeengines.apps.cdl.entitymgr.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.document.repository.writer.DanteConfigWriterRepository;
import com.latticeengines.apps.cdl.entitymgr.DanteConfigEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.documentdb.entity.DanteConfigEntity;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({SimpleRetryListener.class})
public class DanteConfigEntityMgrTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DanteConfigEntityMgrTestNG.class);

    private static final String commonResourcePath = "metadata/";
    private static final String widgetConfigurationDocumentPath = "WidgetConfigurationDocument.json";
    private static final String metadataDocumentTemplatePath = "MetadataDocument.json";
    private static final String UUID_1 = UUID.randomUUID().toString();
    private static final String UUID_2 = UUID.randomUUID().toString();

    @Inject
    private DanteConfigEntityMgr danteConfigEntityMgr;

    @Inject
    private DanteConfigWriterRepository repository;

    private DanteConfigurationDocument danteConfig;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        setupDanteConfiguraiton();
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testSave() {
        createAndUpdateDanteConfig(UUID_1);
        createAndUpdateDanteConfig(UUID_2);
        List<DanteConfigurationDocument> configs = danteConfigEntityMgr.findAllByTenantId(mainCustomerSpace);
        Assert.assertEquals(configs.size(), 2);
    }

    @Test(groups = "functional", dependsOnMethods = "testSave", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testCreate() {
        danteConfigEntityMgr.createOrUpdate(mainCustomerSpace, danteConfig);
        List<DanteConfigurationDocument> configs = danteConfigEntityMgr.findAllByTenantId(mainCustomerSpace);
        Assert.assertEquals(configs.size(), 1);
    }

    private void setupDanteConfiguraiton() {
        String widgetConfigurationDocument = getStaticDocument(commonResourcePath + widgetConfigurationDocumentPath);
        MetadataDocument metadataDocument = JsonUtils.deserialize(
                getStaticDocument(commonResourcePath + metadataDocumentTemplatePath), MetadataDocument.class);
        danteConfig = new DanteConfigurationDocument(metadataDocument, widgetConfigurationDocument);
    }

    private String getStaticDocument(String documentPath) {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream(documentPath);
            return StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e,
                    new String[] { documentPath.replace(commonResourcePath, "") });
        }
    }

    private void createAndUpdateDanteConfig(String uuid) {
        DanteConfigEntity danteConfigEntity = new DanteConfigEntity();
        danteConfigEntity.setUuid(uuid);
        danteConfigEntity.setTenantId(mainCustomerSpace);
        danteConfigEntity.setDocument(danteConfig);
        repository.save(danteConfigEntity);
    }
}
