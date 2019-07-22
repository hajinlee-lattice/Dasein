package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @Type(value = MigrateAccountImportStepConfiguration.class, name ="MigrateAccountImportStepConfiguration"),
        @Type(value = MigrateContactImportStepConfiguration.class, name = "MigrateContactImportStepConfiguration"),
        @Type(value = MigrateTransactionImportStepConfiguration.class, name = "MigrateTransactionImportStepConfiguration")})
public abstract class BaseMigrateImportStepConfiguration extends BaseWrapperStepConfiguration {

}
