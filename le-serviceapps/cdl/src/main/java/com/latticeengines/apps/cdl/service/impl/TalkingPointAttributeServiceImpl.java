package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.cdl.service.TalkingPointAttributeService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointAttributeNotion;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

@Component("talkingPointAttributesService")
public class TalkingPointAttributeServiceImpl implements TalkingPointAttributeService {

    private static final Logger log = LoggerFactory.getLogger(TalkingPointAttributeServiceImpl.class);
    private final String accountAttributePrefix = "Account.";

    @Inject
    private ServingStoreService servingStoreService;

    @VisibleForTesting
    void setServingStoreService(ServingStoreService servingStoreService) {
        this.servingStoreService = servingStoreService;
    }

    @Override
    public List<TalkingPointAttribute> getAccountAttributes() {
        String customerSpace = MultiTenantContext.getShortTenantId();
        log.info("Attempting to find Account attributes for customer space : " + customerSpace);
        try {
            List<ColumnMetadata> allAttrs = servingStoreService.getAccountMetadata(customerSpace, ColumnSelection.Predefined.TalkingPoint, null);

            if (CollectionUtils.isEmpty(allAttrs)) {
                throw new LedpException(LedpCode.LEDP_38023, new String[] { customerSpace });
            }

            // The Prefix is added since Dante UI looks for it to identify
            // Account attributes
            Comparator<TalkingPointAttribute> comparator = Comparator.comparing(TalkingPointAttribute::getName);
            return JsonUtils.convertList(allAttrs, ColumnMetadata.class).stream()
                    .map(attr -> new TalkingPointAttribute(attr.getDisplayName(),
                            accountAttributePrefix + attr.getAttrName(), attr.getCategoryAsString()))
                    .sorted(comparator).collect(Collectors.toList());
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38007, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TalkingPointAttribute> getRecommendationAttributes() {
        String recomendationAttributesFilePath = "com/latticeengines/cdl/talkingpoints/metadata/RecommendationAttributes.json";
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream(recomendationAttributesFilePath);
            String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
            List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
            return JsonUtils.convertList(raw, TalkingPointAttribute.class);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e, new String[] { recomendationAttributesFilePath });
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TalkingPointAttribute> getVariableAttributes() {
        String variableAttributesFilePath = "com/latticeengines/cdl/talkingpoints/metadata/VariableAttributes.json";
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream(variableAttributesFilePath);
            String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
            List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
            return JsonUtils.convertList(raw, TalkingPointAttribute.class);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e, new String[] { variableAttributesFilePath });
        }
    }

    @Override
    public TalkingPointNotionAttributes getAttributesForNotions(List<String> notions) {
        Set<String> uniqueNotions = notions.stream() //
                .map(String::toLowerCase).collect(Collectors.toSet());

        TalkingPointNotionAttributes toReturn = new TalkingPointNotionAttributes();

        for (String notion : uniqueNotions) {
            if (TalkingPointAttributeNotion.isValidDanteNotion(notion)) {
                switch (TalkingPointAttributeNotion.getDanteNotion(notion)) {
                case Account:
                    toReturn.addNotion(notion, getAccountAttributes());
                    break;
                case Recommendation:
                    toReturn.addNotion(notion, getRecommendationAttributes());
                    break;
                case Variable:
                    toReturn.addNotion(notion, getVariableAttributes());
                    break;
                default:
                }
            } else {
                toReturn.addInvalidNotion(notion);
                log.error("Attempted to find attributes for invalid notion " + notion);
            }
        }
        return toReturn;
    }
}
