package com.latticeengines.dante.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.metadata.BaseObjectMetadata;
import com.latticeengines.dante.metadata.MetadataDocument;
import com.latticeengines.dante.metadata.NotionMetadata;
import com.latticeengines.dante.service.AttributeService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("attributesService")
public class AttributeServiceImpl implements AttributeService {

    private static final Logger log = Logger.getLogger(AttributeServiceImpl.class);
    private final Path metadataDocumentPath = new Path("/MetadataDocument.json");
    private final String danteAccountKey = "DanteAccount";
    private final String recomendationAttributesFilePath = "com/latticeengines/dante/metadata/RecommendationAttributes.json";

    @SuppressWarnings("unchecked")
    public Map<String, String> getAccountAttributes(String customerSpace) {
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
    public Map<String, String> getRecommendationAttributes(String customerSpace) {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream(recomendationAttributesFilePath);
            String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
            return JsonUtils.deserialize(attributesDoc, Map.class);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e, new String[] { recomendationAttributesFilePath });
        }
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

    private Map<String, String> getAccountAttributesFromMetadataDocument(String data) {
        try {
            MetadataDocument metadataDoc = JsonUtils.deserialize(data, MetadataDocument.class);
            NotionMetadata danteAccountmetadata = metadataDoc.getNotions().stream()
                    .filter(a -> a.getKey().equals(danteAccountKey)).findFirst().get().getValue();
            return danteAccountmetadata.getProperties().stream()
                    .collect(Collectors.toMap(BaseObjectMetadata::getName, p -> "Account." + p.getName()));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38006, e);
        }
    }
}
