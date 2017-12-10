package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({@JsonSubTypes.Type(value = VdbImportConfig.class, name = "VdbImportConfig"),
                @JsonSubTypes.Type(value = CSVImportConfig.class, name = "CSVImportConfig")})
public class CDLImportConfig {

}