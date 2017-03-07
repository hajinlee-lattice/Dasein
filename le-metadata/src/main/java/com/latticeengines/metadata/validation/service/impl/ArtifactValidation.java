package com.latticeengines.metadata.validation.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.metadata.service.ArtifactValidationService;

@Component
public abstract class ArtifactValidation implements ArtifactValidationService {

    protected static final Log log = LogFactory.getLog(ArtifactValidation.class);

    private static Map<ArtifactType, ArtifactValidationService> registry = new HashMap<>();

    protected ArtifactValidation(ArtifactType artifactType) {
        registry.put(artifactType, this);
    }

    public static ArtifactValidationService getArtifactValidationService(ArtifactType artifactType) {
        return registry.get(artifactType);
    }
}
