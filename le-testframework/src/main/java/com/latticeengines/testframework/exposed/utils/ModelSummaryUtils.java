package com.latticeengines.testframework.exposed.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public class ModelSummaryUtils {

    public static ModelSummary generateModelSummary(Tenant tenant, String modelSummaryJsonLocalResourcePath)
            throws IOException {
        ModelSummary summary;

        summary = new ModelSummary();
        summary.setId("123");
        summary.setName("Model1");
        summary.setRocScore(0.75);
        summary.setLookupId("TENANT1|Q_EventTable_TENANT1|abcde");
        summary.setTrainingRowCount(8000L);
        summary.setTestRowCount(2000L);
        summary.setTotalRowCount(10000L);
        summary.setTrainingConversionCount(80L);
        summary.setTestConversionCount(20L);
        summary.setTotalConversionCount(100L);
        summary.setConstructionTime(System.currentTimeMillis());
        if (summary.getConstructionTime() == null) {
            summary.setConstructionTime(System.currentTimeMillis());
        }
        summary.setLastUpdateTime(summary.getConstructionTime());
        summary.setTenant(tenant);

        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(modelSummaryJsonLocalResourcePath);
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);

        return summary;
    }

    public static ModelSummary createModelSummary(ModelSummaryProxy modelSummaryProxy,
            ColumnMetadataProxy columnMetadataProxy, Tenant tenant,
            ModelSummaryUtils.TestModelConfiguration modelConfiguration) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
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
        String dataCloudVersion = columnMetadataProxy
                .latestVersion(//
                        null)//
                .getVersion();
        modelSummary.setDataCloudVersion(dataCloudVersion);

        ModelSummary retrievedSummary = modelSummaryProxy.getByModelId(modelConfiguration.getModelId());
        if (retrievedSummary != null) {
            modelSummaryProxy.deleteByModelId(customerSpace.toString(), modelConfiguration.getModelId());
        }
        modelSummary.setModelType(modelConfiguration.getModelType());
        modelSummaryProxy.createModelSummary(customerSpace.toString(), modelSummary, false);

        return modelSummary;
    }

    public static class TestModelConfiguration {
        private static final String DUMMY_MODEL_TYPE = "DUMMY_MODEL_TYPE";

        private String modelId;
        private String modelName;
        private String modelType;
        private String localModelPath;
        private String applicationId;
        private String parsedApplicationId;
        private String modelVersion;
        private String eventTable;
        private String sourceInterpretation;
        private String modelSummaryJsonLocalpath;

        public TestModelConfiguration(String modelPath, String modelName, String applicationId, String modelVersion,
                String uuid) {
            this.modelId = TestFrameworkUtils.MODEL_PREFIX + uuid + "_";
            this.modelName = modelName;
            this.applicationId = applicationId;
            this.parsedApplicationId = applicationId.substring(applicationId.indexOf("_") + 1);
            this.modelVersion = modelVersion;
            this.eventTable = modelName;
            this.sourceInterpretation = SchemaInterpretation.SalesforceAccount.name();
            this.localModelPath = modelPath;
            this.modelSummaryJsonLocalpath = localModelPath + Model.MODEL_JSON;
            this.modelType = DUMMY_MODEL_TYPE;
        }

        public String getModelId() {
            return modelId;
        }

        public String getModelName() {
            return modelName;
        }

        public String getLocalModelPath() {
            return localModelPath;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public String getParsedApplicationId() {
            return parsedApplicationId;
        }

        public String getModelVersion() {
            return modelVersion;
        }

        public String getEventTable() {
            return eventTable;
        }

        public String getSourceInterpretation() {
            return sourceInterpretation;
        }

        public String getModelSummaryJsonLocalpath() {
            return modelSummaryJsonLocalpath;
        }

        public String getModelType() {
            return modelType;
        }
    }
}
