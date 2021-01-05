package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.ArrayList;
import java.util.Collection;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.PublishToElasticSearchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToElasticSearchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportTableRoleFromS3StepConfiguration;

public class PublishElasticSearchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private PublishElasticSearchWorkflowConfiguration configuration =
                new PublishElasticSearchWorkflowConfiguration();
        private PublishToElasticSearchStepConfiguration publishToElasticSearchStepConfiguration =
                new PublishToElasticSearchStepConfiguration();
        private ExportToElasticSearchStepConfiguration exportToElasticSearchStepConfiguration =
                new ExportToElasticSearchStepConfiguration();
        private ImportTableRoleFromS3StepConfiguration importTableRoleFromS3StepConfiguration =
                new ImportTableRoleFromS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("publishElasticSearchWorkflow", customerSpace, //
                    configuration.getClass().getSimpleName());
            importTableRoleFromS3StepConfiguration.setCustomerSpace(customerSpace);
            publishToElasticSearchStepConfiguration.setCustomerSpace(customerSpace);
            exportToElasticSearchStepConfiguration.setCustomer(customerSpace.toString());
            return this;
        }

        public Builder tableRoles(Collection<TableRoleInCollection> tableRoles) {
            importTableRoleFromS3StepConfiguration.setTableRoleInCollections(new ArrayList<>(tableRoles));
            publishToElasticSearchStepConfiguration.setTableRoles(new ArrayList<>(tableRoles));
            return this;
        }

        public Builder version(DataCollection.Version version) {
            publishToElasticSearchStepConfiguration.setVersion(version);
            return this;
        }

        public Builder esConfig(ElasticSearchConfig esConfig) {
            exportToElasticSearchStepConfiguration.setEsConfig(esConfig);
            return this;
        }

        public Builder originalTenant(String originalTenant) {
            exportToElasticSearchStepConfiguration.setOriginalTenant(originalTenant);
            return this;
        }

        public PublishElasticSearchWorkflowConfiguration build() {
            configuration.add(importTableRoleFromS3StepConfiguration);
            configuration.add(publishToElasticSearchStepConfiguration);
            configuration.add(exportToElasticSearchStepConfiguration);
            return configuration;
        }
    }
}
