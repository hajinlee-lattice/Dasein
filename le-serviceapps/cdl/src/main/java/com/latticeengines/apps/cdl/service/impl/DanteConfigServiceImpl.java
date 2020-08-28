package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StreamUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.DanteConfigEntityMgr;
import com.latticeengines.apps.cdl.service.DanteConfigService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dante.DanteConfig;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;
import com.latticeengines.domain.exposed.dante.metadata.NotionMetadata;
import com.latticeengines.domain.exposed.dante.metadata.PropertyMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.ActivityProxy;

import reactor.core.publisher.Flux;

@Component("danteConfigService")
public class DanteConfigServiceImpl implements DanteConfigService {

    private static final Logger log = LoggerFactory.getLogger(DanteConfigServiceImpl.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private static final String commonResourcePath = "metadata/";
    private static final String widgetConfigurationDocumentPath = "WidgetConfigurationDocument.json";
    private static final String metadataDocumentTemplatePath = "MetadataDocument.json";
    private static final String salesForceAccountIdAttributeName = "SalesforceAccountID";
    private static final String danteAccountNotionName = "DanteAccount";


    @Inject
    private DanteConfigEntityMgr entityMgr;

    @Override
    public DanteConfig createAndUpdateDanteConfig() {
        String tenantId = MultiTenantContext.getShortTenantId();
        DanteConfig danteConfig = generateDanteConfig();
        return entityMgr.saveAndUpdate(tenantId, danteConfig);
    }

    @Override
    public DanteConfig createAndUpdateDanteConfig(DanteConfig danteConfig) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.saveAndUpdate(tenantId, danteConfig);
    }

    @Override
    public DanteConfig createAndUpdateDanteConfig(String tenantId) {
        DanteConfig danteConfig = generateDanteConfig(tenantId);
        return entityMgr.saveAndUpdate(tenantId, danteConfig);
    }

    @Override
    public void cleanupByTenant() {
        String tenantId = MultiTenantContext.getShortTenantId();
        entityMgr.cleanupTenant(tenantId);
    }

    @Override
    public void cleanupByTenantId(String tenantId) {
        entityMgr.cleanupTenant(tenantId);
    }

    @Override
    public List<DanteConfig> findByTenant() {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.findAllByTenantId(tenantId);
    }

    @Override
    public List<DanteConfig> findByTenant(String tenantId) {
        return entityMgr.findAllByTenantId(tenantId);
    }

    public DanteConfig getDanteConfigByTenantId(String tenantId) {
        List<DanteConfig> danteConfigs = entityMgr.findAllByTenantId(tenantId);
        if (danteConfigs.size() > 1) {
            throw new LedpException(LedpCode.LEDP_38025, new String[] { tenantId });
        }
        if(CollectionUtils.isEmpty(danteConfigs)){
            return createAndUpdateDanteConfig(tenantId);
        }
        return danteConfigs.get(0);
    }


    @Override
    public DanteConfig generateDanteConfig(){
        String customerSpace = MultiTenantContext.getShortTenantId();
        return getDanteConfiguration(customerSpace);
    }

    @Override
    public DanteConfig generateDanteConfig(String tenantId){
        return getDanteConfiguration(tenantId);
    }

    private DanteConfig getDanteConfiguration(String customerSpace){
        String widgetConfigurationDocument = getStaticDocument(commonResourcePath + widgetConfigurationDocumentPath);
        MetadataDocument metadataDocument = JsonUtils.deserialize(
                getStaticDocument(commonResourcePath + metadataDocumentTemplatePath), MetadataDocument.class);
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(customerSpace);
        log.info("version: "+ version.toString());
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
        return new DanteConfig(metadataDocument, widgetConfigurationDocument);
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
