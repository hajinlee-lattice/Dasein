package com.latticeengines.dante.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.service.TalkingPointAttributeService;
import com.latticeengines.domain.exposed.dante.TalkingPointAttribute;
import com.latticeengines.domain.exposed.dante.TalkingPointAttributeNotion;
import com.latticeengines.domain.exposed.dante.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

@Component("talkingPointAttributesService")
public class TalkingPointAttributeServiceImpl implements TalkingPointAttributeService {

    private static final Logger log = LoggerFactory.getLogger(TalkingPointAttributeServiceImpl.class);
    private final String recomendationAttributesFilePath = "com/latticeengines/dante/metadata/RecommendationAttributes.json";
    private final String variableAttributesFilePath = "com/latticeengines/dante/metadata/VariableAttributes.json";
    private final ColumnSelection.Predefined TalkingPointAttributeGroup = ColumnSelection.Predefined.TalkingPoint;
    private final String accountAttributePrefix = "Account.";

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @VisibleForTesting
    void setServingStoreProxy(ServingStoreProxy servingStoreProxy) {
        this.servingStoreProxy = servingStoreProxy;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TalkingPointAttribute> getAccountAttributes(String customerSpace) {
        log.info("Attempting to find Account attributes for customer space : " + customerSpace);
        try {
            List<ColumnMetadata> allAttrs = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace,
                    BusinessEntity.Account);

            if (allAttrs == null || allAttrs.isEmpty()) {
                throw new LedpException(LedpCode.LEDP_38023, new String[] { customerSpace });
            }

            List<ColumnMetadata> rawAttrs = allAttrs.stream().filter(cm -> cm.isEnabledFor(TalkingPointAttributeGroup))
                    .collect(Collectors.toList());

            // The Prefix is added since Dante UI looks for it to identify
            // Account attributes
            return JsonUtils.convertList(rawAttrs, ColumnMetadata.class).stream()
                    .map(attr -> new TalkingPointAttribute(attr.getDisplayName(),
                            accountAttributePrefix + attr.getColumnId()))
                    .collect(Collectors.toList());
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38007, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TalkingPointAttribute> getRecommendationAttributes(String customerSpace) {
        try {
            log.info("Attempting to find Recommendation attributes for customer space : " + customerSpace);
            ClassLoader classLoader = getClass().getClassLoader();
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
    public List<TalkingPointAttribute> getVariableAttributes(String customerSpace) {
        try {
            log.info("Compiling Variable attributes for customer space : " + customerSpace);
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
    @SuppressWarnings("unchecked")
    public TalkingPointNotionAttributes getAttributesForNotions(List<String> notions, String customerSpace) {
        Set<String> uniqueNotions = new HashSet<>(notions.stream() //
                .map(String::toLowerCase) //
                .collect(Collectors.toList()));

        TalkingPointNotionAttributes toReturn = new TalkingPointNotionAttributes();

        for (String notion : uniqueNotions) {
            if (TalkingPointAttributeNotion.isValidDanteNotion(notion)) {
                switch (TalkingPointAttributeNotion.getDanteNotion(notion)) {
                case Account:
                    toReturn.addNotion(notion, getAccountAttributes(customerSpace));
                    break;
                case Recommendation:
                    toReturn.addNotion(notion, getRecommendationAttributes(customerSpace));
                    break;
                case Variable:
                    toReturn.addNotion(notion, getVariableAttributes(customerSpace));
                    break;
                }
            } else {
                toReturn.addInvalidNotion(notion);
                log.error("Attempted to find attributes for invalid notion " + notion);
            }
        }
        return toReturn;
    }
}
