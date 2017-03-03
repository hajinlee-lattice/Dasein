package com.latticeengines.scoringapi.controller;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

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
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.model.ModelJsonTypeHandler;
import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;
import com.latticeengines.scoringapi.functionalframework.ScoringApiTestUtils;
import com.latticeengines.scoringapi.functionalframework.TestModelSummaryParser;
import com.latticeengines.testframework.domain.pls.ModelSummaryUtils;

public class TestRegisterModels {

    public static final String DISPLAY_NAME_PREFIX = "Display Name ";

    public TestModelArtifactDataComposition createModels(Configuration yarnConfiguration,
            InternalResourceRestApiProxy plsRest, Tenant tenant, TestModelConfiguration modelConfiguration,
            CustomerSpace customerSpace, MetadataProxy metadataProxy, TestModelSummaryParser testModelSummaryParser,
            String hdfsSubPathForModel) throws IOException {
        ModelSummary modelSummary = createModel(plsRest, tenant, modelConfiguration, customerSpace,
                testModelSummaryParser);
        createBucketMetadata(plsRest, modelSummary, customerSpace);
        TestModelArtifactDataComposition testModelArtifactDataComposition = setupHdfsArtifacts(yarnConfiguration,
                tenant, modelConfiguration, hdfsSubPathForModel);
        createTableEntryForModel(modelConfiguration.getEventTable(),
                testModelArtifactDataComposition.getEventTableDataComposition().fields, customerSpace, metadataProxy);
        return testModelArtifactDataComposition;
    }

    private void createBucketMetadata(InternalResourceRestApiProxy plsRest, ModelSummary modelSummary,
            CustomerSpace customerSpace) {
        plsRest.createABCDBuckets(modelSummary.getId(), customerSpace,
                ScoringApiTestUtils.generateDefaultBucketMetadataList());
    }

    private ModelSummary createModel(InternalResourceRestApiProxy plsRest, Tenant tenant,
            TestModelConfiguration modelConfiguration, CustomerSpace customerSpace,
            TestModelSummaryParser testModelSummaryParser) throws IOException {
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

        testModelSummaryParser.setPredictors(modelSummary, modelConfiguration.getModelSummaryJsonLocalpath());

        ModelSummary retrievedSummary = plsRest.getModelSummaryFromModelId(modelConfiguration.getModelId(),
                customerSpace);
        if (retrievedSummary != null) {
            plsRest.deleteModelSummary(modelConfiguration.getModelId(), customerSpace);
        }
        modelSummary.setModelType("DUMMY_MODEL_TYPE");
        plsRest.createModelSummary(modelSummary, customerSpace);
        return modelSummary;
    }

    private TestModelArtifactDataComposition setupHdfsArtifacts(Configuration yarnConfiguration, Tenant tenant,
            TestModelConfiguration modelConfiguration, String hdfsSubPathForModel) throws IOException {
        String tenantId = tenant.getId();
        String artifactTableDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR, tenantId,
                modelConfiguration.getEventTable());
        artifactTableDir = artifactTableDir.replaceAll("\\*", hdfsSubPathForModel);

        String artifactBaseDir = String.format(ModelRetrieverImpl.HDFS_SCORE_ARTIFACT_BASE_DIR, tenantId,
                modelConfiguration.getEventTable(), modelConfiguration.getModelVersion(),
                modelConfiguration.getParsedApplicationId());
        String enhancementsDir = artifactBaseDir + ModelJsonTypeHandler.HDFS_ENHANCEMENTS_DIR;

        InputStream eventTableDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(modelConfiguration.getLocalModelPath()
                + "eventtable-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        InputStream modelJsonUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(modelConfiguration.getModelSummaryJsonLocalpath());
        InputStream rfpmmlUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(modelConfiguration.getLocalModelPath() + ModelJsonTypeHandler.PMML_FILENAME);
        InputStream dataScienceDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(modelConfiguration.getLocalModelPath()
                + "datascience-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        InputStream scoreDerivationUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(
                modelConfiguration.getLocalModelPath() + ModelJsonTypeHandler.SCORE_DERIVATION_FILENAME);

        HdfsUtils.rmdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.rmdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.rmdir(yarnConfiguration, enhancementsDir);

        HdfsUtils.mkdir(yarnConfiguration, artifactTableDir);
        HdfsUtils.mkdir(yarnConfiguration, artifactBaseDir);
        HdfsUtils.mkdir(yarnConfiguration, enhancementsDir);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, eventTableDataCompositionUrl,
                artifactTableDir + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, modelJsonUrl,
                artifactBaseDir + modelConfiguration.getTestModelFolderName() + "_model.json");
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, rfpmmlUrl,
                artifactBaseDir + ModelJsonTypeHandler.PMML_FILENAME);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, dataScienceDataCompositionUrl,
                enhancementsDir + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, scoreDerivationUrl,
                enhancementsDir + ModelJsonTypeHandler.SCORE_DERIVATION_FILENAME);

        eventTableDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(modelConfiguration.getLocalModelPath()
                        + "eventtable-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        String eventTableDataCompositionContents = IOUtils.toString(eventTableDataCompositionUrl,
                Charset.defaultCharset());
        TestModelArtifactDataComposition testModelArtifactDataComposition = new TestModelArtifactDataComposition();
        testModelArtifactDataComposition.setEventTableDataComposition(
                JsonUtils.deserialize(eventTableDataCompositionContents, DataComposition.class));

        dataScienceDataCompositionUrl = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(modelConfiguration.getLocalModelPath()
                        + "datascience-" + ModelJsonTypeHandler.DATA_COMPOSITION_FILENAME);
        String dataScienceDataCompositionContents = IOUtils.toString(dataScienceDataCompositionUrl,
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

}
