package com.latticeengines.scoringapi.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.scoringapi.exposed.InternalResourceRestApiProxy;
import com.latticeengines.scoringapi.exposed.model.impl.ModelRetrieverImpl;
import com.latticeengines.testframework.domain.pls.ModelSummaryUtils;

public class TestRegisterModels {

    public TestModelArtifactDataComposition createModels(Configuration yarnConfiguration,
            InternalResourceRestApiProxy plsRest, Tenant tenant, TestModelConfiguration modelConfiguration,
            CustomerSpace customerSpace) throws IOException {
        createModel(plsRest, tenant, modelConfiguration, customerSpace);
        return setupHdfsArtifacts(yarnConfiguration, tenant, modelConfiguration);
    }

    private void createModel(InternalResourceRestApiProxy plsRest, Tenant tenant,
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

}
