package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;

public interface DataMappingService {
    FetchFieldDefinitionsResponse fetchFieldDefinitions(String systemName, String systemType, String systemObject,
                                                        String importFile) throws Exception;

    FieldDefinitionsRecord commitFieldDefinitions(String systemName, String systemType, String systemObject,
                                                  String importFile, boolean runImport,
                                                  FieldDefinitionsRecord commitRequest)
            throws LedpException, IllegalArgumentException ;

    ValidateFieldDefinitionsResponse validateFieldDefinitions(String systemName, String systemType,
                                                              String systemObject, String importFile,
                                                              ValidateFieldDefinitionsRequest validateRequest) throws Exception;
}
