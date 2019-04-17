package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SORTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_STANDARDIZATION;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SorterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(ProfileProduct.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProfileProduct extends BaseSingleEntityProfileStep<ProcessProductStepConfiguration> {

    public static final String BEAN_NAME = "profileProduct";

    private String masterTableName;

    @Override
    protected TableRoleInCollection profileTableRole() {
        return null;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String servingStoreTableName = TableUtils.getFullTableName(servingStoreTablePrefix, pipelineVersion);
        Table servingStoreTable = metadataProxy.getTable(customerSpace.toString(), servingStoreTableName);
        servingStoreTableName = renameServingStoreTable(servingStoreTable);

        exportTableRoleToRedshift(servingStoreTableName, getEntity().getServingStore());
        dataCollectionProxy.upsertTable(customerSpace.toString(), servingStoreTableName, getEntity().getServingStore(),
                inactive);
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        masterTableName = masterTable.getName();
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateProductStep");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);

        TransformationStepConfig standardization = standardization();
        TransformationStepConfig sort = sort();
        // -----------
        List<TransformationStepConfig> steps = Arrays.asList(standardization, sort);
        // -----------
        request.setSteps(steps);
        return request;
    }

    private TransformationStepConfig standardization() {
        TransformationStepConfig step = new TransformationStepConfig();
        String tableSourceName = "CustomerUniverse";
        SourceTable sourceTable = new SourceTable(masterTableName, customerSpace);
        List<String> baseSources = Collections.singletonList(tableSourceName);
        step.setBaseSources(baseSources);
        Map<String, SourceTable> baseTables = new HashMap<>();
        baseTables.put(tableSourceName, sourceTable);
        step.setBaseTables(baseTables);
        step.setTransformer(TRANSFORMER_STANDARDIZATION);

        StandardizationTransformerConfig transformerConfig = new StandardizationTransformerConfig();
        StandardizationTransformerConfig.StandardizationStrategy[] strategies = new StandardizationTransformerConfig.StandardizationStrategy[] {
                StandardizationTransformerConfig.StandardizationStrategy.FILTER };
        transformerConfig.setSequence(strategies);
        transformerConfig
                .setFilterFields(new String[] { InterfaceName.ProductType.name(), InterfaceName.ProductStatus.name() });
        String filterExpression = String.format("%s.equalsIgnoreCase(\"%s\") && !%s.equalsIgnoreCase(\"%s\")",
                InterfaceName.ProductType.name(), ProductType.Analytic.name(), InterfaceName.ProductStatus.name(),
                ProductStatus.Obsolete.name());
        transformerConfig.setFilterExpression(filterExpression);
        step.setConfiguration(appendEngineConf(transformerConfig, lightEngineConfig()));

        return step;
    }

    private TransformationStepConfig sort() {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(0));
        step.setTransformer(TRANSFORMER_SORTER);

        SorterConfig config = new SorterConfig();
        config.setPartitions(20);
        String sortingKey = TableRoleInCollection.SortedProduct.getForeignKeysAsStringList().get(0);
        config.setSortingField(sortingKey);
        config.setCompressResult(true);
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(servingStoreTablePrefix);
        step.setTargetTable(targetTable);

        return step;
    }

}
