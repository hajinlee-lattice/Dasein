package com.latticeengines.scoringapi.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scoringapi.exposed.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;
import com.latticeengines.testframework.domain.pls.ModelSummaryUtils;

public class TestRegisterModels {

    public static final String DISPLAY_NAME_PREFIX = "Display Name ";

    public TestModelArtifactDataComposition createModels(Configuration yarnConfiguration,
            InternalResourceRestApiProxy plsRest, Tenant tenant, TestModelConfiguration modelConfiguration,
            CustomerSpace customerSpace, MetadataProxy metadataProxy) throws IOException {
        createModel(plsRest, tenant, modelConfiguration, customerSpace);
        TestModelArtifactDataComposition testModelArtifactDataComposition = setupHdfsArtifacts(yarnConfiguration,
                tenant, modelConfiguration);
        createTableEntryForModel(modelConfiguration.getEventTable(),
                testModelArtifactDataComposition.getEventTableDataComposition().fields, customerSpace, metadataProxy);
        return testModelArtifactDataComposition;
    }

    private ModelSummary createModel(InternalResourceRestApiProxy plsRest, Tenant tenant,
            TestModelConfiguration modelConfiguration, CustomerSpace customerSpace) throws IOException {
        ModelSummary modelSummary = ModelSummaryUtils.generateModelSummary(tenant,
                modelConfiguration.getModelSummaryJsonLocalpath());
        modelSummary.setApplicationId(modelConfiguration.getApplicationId());
        modelSummary.setEventTableName(modelConfiguration.getEventTable());
        modelSummary.setId(modelConfiguration.getModelId());
        modelSummary.setName(modelConfiguration.getModelName());
        modelSummary.setDisplayName(modelConfiguration.getModelName());
        modelSummary.setLookupId(String.format("%s|%s|%s", tenant.getId(), modelConfiguration.getEventTable(),
                modelConfiguration.getModelVersion()));
        modelSummary.setSourceSchemaInterpretation(modelConfiguration.getSourceInterpretation());
        modelSummary.setStatus(ModelSummaryStatus.ACTIVE);

        ModelSummary retrievedSummary = plsRest.getModelSummaryFromModelId(modelConfiguration.getModelId(),
                customerSpace);
        if (retrievedSummary != null) {
            plsRest.deleteModelSummary(modelConfiguration.getModelId(), customerSpace);
        }
        plsRest.createModelSummary(modelSummary, customerSpace);
        return modelSummary;
    }

    private TestModelArtifactDataComposition setupHdfsArtifacts(Configuration yarnConfiguration, Tenant tenant,
            TestModelConfiguration modelConfiguration) throws IOException {
        String tenantId = tenant.getId();
        String artifactTableDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR, tenantId,
                modelConfiguration.getEventTable());
        String artifactBaseDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_BASE_DIR, tenantId,
                modelConfiguration.getEventTable(), modelConfiguration.getModelVersion(),
                modelConfiguration.getParsedApplicationId());
        String enhancementsDir = artifactBaseDir + ModelRetrieverImpl.HDFS_ENHANCEMENTS_DIR;

        URL eventTableDataCompositionUrl = ClassLoader.getSystemResource(
                modelConfiguration.getLocalModelPath() + "eventtable-" + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        URL modelJsonUrl = ClassLoader.getSystemResource(modelConfiguration.getModelSummaryJsonLocalpath());
        URL rfpmmlUrl = ClassLoader
                .getSystemResource(modelConfiguration.getLocalModelPath() + ModelRetrieverImpl.PMML_FILENAME);
        URL dataScienceDataCompositionUrl = ClassLoader.getSystemResource(
                modelConfiguration.getLocalModelPath() + "datascience-" + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        URL scoreDerivationUrl = ClassLoader.getSystemResource(
                modelConfiguration.getLocalModelPath() + ModelRetrieverImpl.SCORE_DERIVATION_FILENAME);

        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);

        HdfsUtils.mkdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.mkdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.mkdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, eventTableDataCompositionUrl.getFile(),
                artifactTableDir + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, modelJsonUrl.getFile(),
                artifactBaseDir + modelConfiguration.getTestModelFolderName() + "_model.json");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, rfpmmlUrl.getFile(),
                artifactBaseDir + ModelRetrieverImpl.PMML_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataScienceDataCompositionUrl.getFile(),
                enhancementsDir + ModelRetrieverImpl.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, scoreDerivationUrl.getFile(),
                enhancementsDir + ModelRetrieverImpl.SCORE_DERIVATION_FILENAME);

        String eventTableDataCompositionContents = Files.toString(new File(eventTableDataCompositionUrl.getFile()),
                Charset.defaultCharset());
        TestModelArtifactDataComposition testModelArtifactDataComposition = new TestModelArtifactDataComposition();
        testModelArtifactDataComposition.setEventTableDataComposition(
                JsonUtils.deserialize(eventTableDataCompositionContents, DataComposition.class));

        String dataScienceDataCompositionContents = Files.toString(new File(dataScienceDataCompositionUrl.getFile()),
                Charset.defaultCharset());
        testModelArtifactDataComposition.setDataScienceDataComposition(
                JsonUtils.deserialize(dataScienceDataCompositionContents, DataComposition.class));
        return testModelArtifactDataComposition;
    }

    public void deleteModel(InternalResourceRestApiProxy plsRest, CustomerSpace customerSpace, String modelId) {
        plsRest.deleteModelSummary(modelId, customerSpace);
    }

    public static void createTableEntryForModel(String tableName, Map<String, FieldSchema> fields,
            CustomerSpace customerSpace, MetadataProxy metadataProxy) throws IOException {
        Table scoreResultTable = createGenericOutputSchema(tableName, fields);
        metadataProxy.createTable(customerSpace.toString(), scoreResultTable.getName(), scoreResultTable);
    }

    private static Table createGenericOutputSchema(String tableName, Map<String, FieldSchema> fields) {
        Table scoreResultTable = new Table();
        scoreResultTable.setName(tableName);
        scoreResultTable.setDisplayName(tableName);
        List<Attribute> attributes = new ArrayList<>();
        for (String key : fields.keySet()) {
            Attribute attr = new Attribute();
            attr.setName(key);
            attr.setDisplayName(DISPLAY_NAME_PREFIX + key);
            Type type = null;
            switch (fields.get(key).type) {
            case BOOLEAN:
                type = Type.BOOLEAN;
                break;
            case FLOAT:
                type = Type.FLOAT;
                break;
            case INTEGER:
                type = Type.INT;
                break;
            case LONG:
                type = Type.LONG;
                break;
            case STRING:
            default:
                type = Type.STRING;
                break;
            }
            attr.setPhysicalDataType(type.name());
            attr.setSourceLogicalDataType(fields.get(key).type.name());
            attributes.add(attr);
        }

        scoreResultTable.setAttributes(attributes);
        return scoreResultTable;
    }

    private static void createTable(String name, Table table, CustomerSpace customerSpace,
            MetadataProxy metadataProxy) {

    }
}
