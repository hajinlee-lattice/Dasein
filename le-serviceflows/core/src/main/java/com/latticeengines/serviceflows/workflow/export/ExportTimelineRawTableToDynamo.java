package com.latticeengines.serviceflows.workflow.export;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportTimelineRawTableToDynamoStepConfiguration;

@Component("exportTimelineRawTableToDynamo")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportTimelineRawTableToDynamo extends BaseExportToDynamo<ExportTimelineRawTableToDynamoStepConfiguration> {

}
