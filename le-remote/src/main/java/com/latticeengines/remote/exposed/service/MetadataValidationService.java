package com.latticeengines.remote.exposed.service;

import com.latticeengines.remote.exposed.exception.MetadataValidationException;

public interface MetadataValidationService {
    public void validate(String metadata) throws MetadataValidationException;
}
