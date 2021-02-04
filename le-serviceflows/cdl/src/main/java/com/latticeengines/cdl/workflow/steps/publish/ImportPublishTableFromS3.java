package com.latticeengines.cdl.workflow.steps.publish;

import static com.latticeengines.cdl.workflow.steps.publish.ImportPublishTableFromS3.BEAN_NAME;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.publish.ImportPublishTableFromS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;


@Component(BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class ImportPublishTableFromS3 extends BaseImportExportS3<ImportPublishTableFromS3StepConfiguration> {

    static final String BEAN_NAME = "importPublishTableFromS3";

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {

        List<ElasticSearchExportConfig> exportConfigs = configuration.getExportConfigs();
        if (CollectionUtils.isEmpty(exportConfigs)) {
            return ;
        }
        for(ElasticSearchExportConfig config : exportConfigs) {
            String tableName = config.getTableName();
            if (StringUtils.isNotBlank(tableName)) {
                Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
                if (table != null) {
                    addTableToRequestForImport(table, requests);
                }

            }
        }

    }
}
