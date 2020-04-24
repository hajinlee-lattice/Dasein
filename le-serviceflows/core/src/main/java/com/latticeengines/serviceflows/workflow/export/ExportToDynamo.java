package com.latticeengines.serviceflows.workflow.export;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;

@Component("exportToDynamo")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportToDynamo extends BaseExportToDynamo<ExportToDynamoStepConfiguration> {

}
