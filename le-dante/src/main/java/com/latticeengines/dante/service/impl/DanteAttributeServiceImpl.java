package com.latticeengines.dante.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.metadata.MetadataDocument;
import com.latticeengines.dante.metadata.NotionMetadata;
import com.latticeengines.dante.service.DanteAttributeService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteAttributeNotion;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("danteAttributesService")
public class DanteAttributeServiceImpl implements DanteAttributeService {

    private static final Logger log = LoggerFactory.getLogger(DanteAttributeServiceImpl.class);
    private final Path metadataDocumentPath = new Path("/MetadataDocument.json");
    private final String danteAccountKey = "DanteAccount";
    private final String recomendationAttributesFilePath = "com/latticeengines/dante/metadata/RecommendationAttributes.json";
    private final String variableAttributesFilePath = "com/latticeengines/dante/metadata/VariableAttributes.json";

    @SuppressWarnings("unchecked")
    @Override
    public List<DanteAttribute> getAccountAttributes(String customerSpace) {
        log.info("Attempting to find Account attributes for customerspace : " + customerSpace);
        try {
            Document doc = getMetadataDocument(CustomerSpace.parse(customerSpace));
            return getAccountAttributesFromMetadataDocument(doc.getData());
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38007, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DanteAttribute> getRecommendationAttributes(String customerSpace) {
        try {
            log.info("Attempting to find Recommendation attributes for customerspace : " + customerSpace);
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream(recomendationAttributesFilePath);
            String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
            List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
            return JsonUtils.convertList(raw, DanteAttribute.class);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e, new String[] { recomendationAttributesFilePath });
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DanteAttribute> getVariableAttributes(String customerSpace) {
        try {
            log.info("Compiling Variable attributes for customerspace : " + customerSpace);
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream(variableAttributesFilePath);
            String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
            List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
            return JsonUtils.convertList(raw, DanteAttribute.class);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e, new String[] { variableAttributesFilePath });
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public DanteNotionAttributes getAttributesForNotions(List<String> notions, String customerSpace) {
        Set<String> uniqueNotions = new HashSet<>(notions.stream() //
                .map(String::toLowerCase) //
                .collect(Collectors.toList()));

        DanteNotionAttributes toReturn = new DanteNotionAttributes();

        for (String notion : uniqueNotions) {
            if (DanteAttributeNotion.isValidDanteNotion(notion)) {
                switch (DanteAttributeNotion.getDanteNotion(notion)) {
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

    private Document getMetadataDocument(CustomerSpace customerSpace) {
        Camille camille = CamilleEnvironment.getCamille();

        try {
            Path docPath = PathBuilder
                    .buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                            customerSpace.getTenantId(), customerSpace.getSpaceId())
                    .append(PathConstants.SERVICES) //
                    .append(PathConstants.DANTE) //
                    .append(metadataDocumentPath);
            log.info("Attempting to retrieve metadata document " + docPath.toString());
            return camille.get(docPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38005, e, new String[] { customerSpace.toString() });
        }

    }

    private List<DanteAttribute> getAccountAttributesFromMetadataDocument(String data) {
        try {
            MetadataDocument metadataDoc = JsonUtils.deserialize(data, MetadataDocument.class);
            NotionMetadata danteAccountmetadata = metadataDoc.getNotions().stream()
                    .filter(a -> a.getKey().equals(danteAccountKey)).findFirst().get().getValue();
            return danteAccountmetadata.getProperties().stream()
                    .map(acc -> new DanteAttribute(acc.getName(), "Account." + acc.getName()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38006, e);
        }
    }
}
