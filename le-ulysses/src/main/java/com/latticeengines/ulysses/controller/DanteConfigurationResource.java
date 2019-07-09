package com.latticeengines.ulysses.controller;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dante.metadata.MetadataDocument;
import com.latticeengines.domain.exposed.dante.metadata.NotionMetadata;
import com.latticeengines.domain.exposed.dante.metadata.PropertyMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.FrontEndResponse;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Flux;

@Api(value = "DanteConfiguration", description = "Common REST resource to serve configuration for Dante UI")
@RestController
@RequestMapping("/danteconfiguration")
public class DanteConfigurationResource {
    private static final Logger log = LoggerFactory.getLogger(DanteConfigurationResource.class);

    private static class FrontEndDanteConfigurationDocument {

        private MetadataDocument metadataDocument;
        private String widgetConfigurationDocument;

        @JsonProperty("MetadataDocument")
        public MetadataDocument getMetadataDocument() {
            return metadataDocument;
        }

        @JsonProperty("WidgetConfigurationDocument")
        public String getWidgetConfigurationDocument() {
            return widgetConfigurationDocument;
        }

        FrontEndDanteConfigurationDocument(MetadataDocument metadataDocument, String widgetConfigurationDocument) {
            this.metadataDocument = metadataDocument;
            this.widgetConfigurationDocument = widgetConfigurationDocument;
        }

    }

    private static final String commonResourcePath = "metadata/";
    private static final String widgetConfigurationDocumentPath = "WidgetConfigurationDocument.json";
    private static final String metadataDocumentTemplatePath = "MetadataDocument.json";
    private static final String salesForceAccountIdAttributeName = "SalesforceAccountID";
    private static final String danteAccountNotionName = "DanteAccount";

    private final ColumnSelection.Predefined TalkingPointAttributeGroup = ColumnSelection.Predefined.TalkingPoint;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private EntityProxy entityProxy;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get an account by of attributes in a group")
    public FrontEndResponse<FrontEndDanteConfigurationDocument> getDanteConfiguration() {
        String customerSpace = MultiTenantContext.getShortTenantId();
        try {
            String widgetConfigurationDocument = getStaticDocument(
                    commonResourcePath + widgetConfigurationDocumentPath);
            MetadataDocument metadataDocument = JsonUtils.deserialize(
                    getStaticDocument(commonResourcePath + metadataDocumentTemplatePath), MetadataDocument.class);

            List<ColumnMetadata> allAttrs = new ArrayList<>();
            List<ColumnMetadata> accountAttrs = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace,
                    BusinessEntity.Account);
            if (CollectionUtils.isNotEmpty(accountAttrs)) {
                accountAttrs = accountAttrs.stream().filter(cm -> cm.isEnabledFor(TalkingPointAttributeGroup))
                        .collect(Collectors.toList());
                allAttrs.addAll(accountAttrs);
            }
            List<ColumnMetadata> ratingAttrs = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace,
                    BusinessEntity.Rating);
            if (CollectionUtils.isNotEmpty(ratingAttrs)) {
                ratingAttrs = ratingAttrs.stream().filter(cm -> cm.isEnabledFor(TalkingPointAttributeGroup))
                        .collect(Collectors.toList());
                allAttrs.addAll(ratingAttrs);
            }
            List<ColumnMetadata> phAttrs = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace,
                    BusinessEntity.PurchaseHistory);
            if (CollectionUtils.isNotEmpty(phAttrs)) {
                DataPage dataPage = entityProxy.getProducts(customerSpace);
                Map<String, String> productNameMap = new HashMap<>();
                if (dataPage != null && CollectionUtils.isNotEmpty(dataPage.getData())) {
                    dataPage.getData()
                            .forEach(map -> productNameMap.put( //
                                    map.get(InterfaceName.ProductId.name()).toString(), //
                                    map.get(InterfaceName.ProductName.name()).toString() //
                    ));
                }
                phAttrs = phAttrs.stream().filter(cm -> cm.isEnabledFor(TalkingPointAttributeGroup)).peek(cm -> {
                    String productId = ActivityMetricsUtils.getProductIdFromFullName(cm.getAttrName());
                    String productName = productNameMap.get(productId);
                    cm.setDisplayName(productName + ": " + cm.getDisplayName());
                }).collect(Collectors.toList());
                allAttrs.addAll(phAttrs);
            }
            List<ColumnMetadata> curatedAccountAttrs = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace,
                    BusinessEntity.CuratedAccount);
            if (CollectionUtils.isNotEmpty(curatedAccountAttrs)) {
                curatedAccountAttrs = curatedAccountAttrs.stream()
                        .filter(cm -> cm.isEnabledFor(TalkingPointAttributeGroup)).collect(Collectors.toList());
                allAttrs.addAll(curatedAccountAttrs);
            }

            if (CollectionUtils.isEmpty(allAttrs)) {
                throw new LedpException(LedpCode.LEDP_38023, new String[] { customerSpace });
            }
            List<PropertyMetadata> talkingPointAttributes = Flux.fromIterable(allAttrs)
                    // Dante has a special meaning for "SalesforceAccountID"
                    // so ignore the attribute info from CDL
                    .filter(attr -> !attr.getAttrName().equals(salesForceAccountIdAttributeName))
                    .map(PropertyMetadata::new).collectList().block();

            log.info("Found " + CollectionUtils.size(talkingPointAttributes) + " talking point attributes " //
                    + "for the tenant " + customerSpace);

            NotionMetadata danteAccountMetadata = metadataDocument.getNotions().stream()
                    .filter(notion -> notion.getKey().equals(danteAccountNotionName)).collect(Collectors.toList())
                    .get(0).getValue();
            danteAccountMetadata.getProperties().addAll(talkingPointAttributes);

            return new FrontEndResponse<>(new FrontEndDanteConfigurationDocument( //
                    metadataDocument, widgetConfigurationDocument));
        } catch (LedpException le) {
            log.error("Failed to get talking point data", le);
            return new FrontEndResponse<>(le.getErrorDetails());
        } catch (Exception e) {
            log.error("Failed to get talking point data", e);
            return new FrontEndResponse<>(new LedpException(LedpCode.LEDP_00002, e).getErrorDetails());
        }
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

}
