package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.DanteConfigEntityMgr;
import com.latticeengines.apps.cdl.service.DanteConfigService;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;
import com.latticeengines.domain.exposed.dante.metadata.NotionMetadata;
import com.latticeengines.domain.exposed.dante.metadata.PropertyMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

@Component("danteConfigService")
public class DanteConfigServiceImpl implements DanteConfigService {

    private static final Logger log = LoggerFactory.getLogger(DanteConfigServiceImpl.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataCollectionService dataCollectionService;

    private static final String commonResourcePath = "metadata/";
    private static final String widgetConfigurationDocumentPath = "WidgetConfigurationDocument.json";
    private static final String metadataDocumentTemplatePath = "MetadataDocument.json";
    private static final String salesForceAccountIdAttributeName = "SalesforceAccountID";
    private static final String danteAccountNotionName = "DanteAccount";

    @Inject
    private DanteConfigEntityMgr entityMgr;

    @Override
    public DanteConfigurationDocument getDanteConfiguration() {
        try {
            String tenantId = MultiTenantContext.getShortTenantId();
            DanteConfigurationDocument danteConfig = entityMgr.findByTenantId(tenantId);
            if (danteConfig == null) {
                log.warn("No Pre-populated Dante Configuration document found for Tenant=" + tenantId
                        + " Rebuilding now.");
                return createAndUpdateDanteConfig();
            } else {
                log.info("Dante config found for Tenant=" + tenantId);
            }
            return danteConfig;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public DanteConfigurationDocument refreshDanteConfiguration() {
        String tenantId = MultiTenantContext.getShortTenantId();
        log.info("Refreshing Dante Configuration document for Tenant=" + tenantId);
        return createAndUpdateDanteConfig();
    }

    private DanteConfigurationDocument createAndUpdateDanteConfig() {
        String tenantId = MultiTenantContext.getShortTenantId();
        try (PerformanceTimer timer = new PerformanceTimer(
                "Create and update Danteconfiguration for Tenant=" + tenantId, log)) {
            DanteConfigurationDocument danteConfig = generateDanteConfiguration(tenantId);
            return entityMgr.createOrUpdate(tenantId, danteConfig);
        }
    }

    private DanteConfigurationDocument generateDanteConfiguration(String customerSpace) {
        String widgetConfigurationDocument = getStaticDocument(commonResourcePath + widgetConfigurationDocumentPath);
        MetadataDocument metadataDocument = JsonUtils.deserialize(
                getStaticDocument(commonResourcePath + metadataDocumentTemplatePath), MetadataDocument.class);
        DataCollection.Version version = dataCollectionService.getActiveVersion(customerSpace);
        List<ColumnMetadata> allAttrs = servingStoreProxy.getAccountMetadata(customerSpace,
                ColumnSelection.Predefined.TalkingPoint, version);
        if (CollectionUtils.isEmpty(allAttrs)) {
            throw new LedpException(LedpCode.LEDP_38023, new String[] { customerSpace });
        }
        List<PropertyMetadata> talkingPointAttributes = Flux.fromIterable(allAttrs)
                // Dante has a special meaning for "SalesforceAccountID"
                // so ignore the attribute info from CDL
                .filter(attr -> !attr.getAttrName().equals(salesForceAccountIdAttributeName)).map(PropertyMetadata::new)
                .collectList().block();

        log.info("Found " + CollectionUtils.size(talkingPointAttributes) + " talking point attributes " //
                + "for the tenant " + customerSpace);

        NotionMetadata danteAccountMetadata = metadataDocument.getNotions().stream()
                .filter(notion -> notion.getKey().equals(danteAccountNotionName)).collect(Collectors.toList()).get(0)
                .getValue();
        danteAccountMetadata.getProperties().addAll(talkingPointAttributes);
        return new DanteConfigurationDocument(metadataDocument, widgetConfigurationDocument);
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

    @VisibleForTesting
    void setServingStoreProxy(ServingStoreProxy servingStoreProxy) {
        this.servingStoreProxy = servingStoreProxy;
    }

}
