package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
    @Type(value = InputFileValidatorConfiguration.class, name = "InputFileValidatorConfiguration"),
})
public class BaseInputFileValidatorConfiguration extends MicroserviceStepConfiguration {

}
