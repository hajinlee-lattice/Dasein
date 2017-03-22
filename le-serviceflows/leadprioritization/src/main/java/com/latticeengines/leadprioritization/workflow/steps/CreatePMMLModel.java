package com.latticeengines.leadprioritization.workflow.steps;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dmg.pmml.DataField;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.jpmml.model.JAXBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.KV;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.pmml.PmmlField;
import com.latticeengines.domain.exposed.pmml.RegressionTargetCorrector;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.util.ModelingUtils;
import com.latticeengines.domain.exposed.util.PmmlModelUtils;
import com.latticeengines.leadprioritization.workflow.steps.pmml.PMMLModelingServiceExecutor;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("createPMMLModel")
public class CreatePMMLModel extends BaseWorkflowStep<CreatePMMLModelConfiguration> {

    private static final Log log = LogFactory.getLog(CreatePMMLModel.class);

    @Autowired
    private JobProxy jobProxy;

    @Autowired
    private ModelProxy modelProxy;

    @Override
    public void execute() {
        log.info("Inside CreatePMMLModel execute()");

        try {
            PMMLModelingServiceExecutor.Builder builder = createModelingServiceExecutorBuilder(configuration);
            PMMLModelingServiceExecutor executor = new PMMLModelingServiceExecutor(builder);
            executor.writeMetadataFiles();
            executor.writeDataFiles();
            String appId = executor.model();
            Map<String, String> modelApplicationIdToEventColumn = new HashMap<>();
            modelApplicationIdToEventColumn.put(appId, builder.getTargets()[0]);
            putObjectInContext(MODEL_APP_IDS, modelApplicationIdToEventColumn);
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new LedpException(LedpCode.LEDP_28019, new String[] { configuration.getPmmlArtifactPath() });
        }
    }

    protected PMMLModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            CreatePMMLModelConfiguration modelStepConfiguration) throws Exception {
        String pmmlPath = configuration.getPmmlArtifactPath();
        InputStream pmmlStream = HdfsUtils.getInputStream(yarnConfiguration, pmmlPath);
        List<PmmlField> pmmlFields = PmmlModelUtils.getPmmlFields(pmmlStream);

        PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration,
                configuration.getPivotArtifactPath());

        String metadataContents = getMetadataContents(pmmlFields, pivotValues);
        String datacompositionContents = getDataCompositionContents(pmmlFields, pivotValues);
        String avroSchema = getAvroSchema(pmmlFields, pivotValues);

        Map.Entry<String[], String> featuresAndTarget = getFeaturesAndTarget(pmmlFields, pivotValues);
        PMMLModelingServiceExecutor.Builder bldr = new PMMLModelingServiceExecutor.Builder();
        String tableName = "PMMLDummyTable-" + System.currentTimeMillis();

        bldr.modelingServiceHostPort(modelStepConfiguration.getMicroServiceHostPort()) //
                .modelingServiceHdfsBaseDir(modelStepConfiguration.getModelingServiceHdfsBaseDir()) //
                .customer(modelStepConfiguration.getCustomerSpace().toString()) //
                .metadataContents(metadataContents) //
                .dataCompositionContents(datacompositionContents) //
                .featureList(featuresAndTarget.getKey()) //
                .targets(featuresAndTarget.getValue()) //
                .jobProxy(jobProxy) //
                .modelProxy(modelProxy) //
                .modelName(modelStepConfiguration.getModelName()) //
                .table(tableName) //
                .metadataTable(String.format("%s-Event-Metadata", tableName)) //
                .avroSchema(avroSchema) //
                .metadataArtifacts(getMetadataArtifacts()) //
                .sourceSchemaInterpretation(configuration.getSourceSchemaInterpretation()) //
                .moduleName(configuration.getModuleName()) //
                .pivotArtifactPath(configuration.getPivotArtifactPath()) //
                .yarnConfiguration(yarnConfiguration);
        if (configuration.getDisplayName() != null) {
            bldr = bldr.displayName(configuration.getDisplayName());
        }
        return bldr;
    }

    private void fixPMMLFile(String pmmlPath, String fixedPmmlPath) throws Exception {
        PMML pmml = PmmlModelUtils.getPMML(HdfsUtils.getInputStream(yarnConfiguration, pmmlPath), false);
        new RegressionTargetCorrector().applyTo(pmml);
        try (FileSystem fs = HdfsUtils.getFileSystem(yarnConfiguration)) {
            try (OutputStream stream = new BufferedOutputStream(fs.create(new Path(fixedPmmlPath)))) {
                Result result = new StreamResult(stream);
                JAXBUtil.marshalPMML(pmml, result);
            }
        }
    }

    Map<ArtifactType, String> getMetadataArtifacts() {
        Map<ArtifactType, String> metadataArtifacts = new HashMap<>();
        String fixedPmmlPath = configuration.getPmmlArtifactPath() + ".fixed.xml";
        try {
            fixPMMLFile(configuration.getPmmlArtifactPath(), fixedPmmlPath);
            metadataArtifacts.put(ArtifactType.PMML, fixedPmmlPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return metadataArtifacts;
    }

    @VisibleForTesting
    final String getAvroSchema(List<PmmlField> pmmlFields, PivotValuesLookup pivotValues) throws Exception {
        DataSchema dataSchema = new DataSchema();
        dataSchema.setName("PMMLDummyTableSchema");
        dataSchema.setType("record");

        Map<String, AbstractMap.Entry<String, List<String>>> pivotValuesByTargetColumn = pivotValues.pivotValuesByTargetColumn;
        Map<String, List<AbstractMap.Entry<String, String>>> pivotValuesBySourceColumn = pivotValues.pivotValuesBySourceColumn;
        Map<String, UserDefinedType> sourceColumnTypes = pivotValues.sourceColumnToUserType;

        for (PmmlField pmmlField : pmmlFields) {
            DataField dataField = pmmlField.dataField;

            if (dataField == null && pmmlField.miningField.getUsageType() != FieldUsageType.PREDICTED) {
                continue;
            }

            Field field = new Field();
            if (dataField != null) {
                String name = dataField.getName().getValue();
                if (pivotValuesByTargetColumn.containsKey(name)) {
                    continue;
                }

                field.setName(name);
                FieldType fieldType = FieldType.getFromPmmlType(pmmlField.dataField.getDataType());
                field.setType(Arrays.asList(fieldType.avroTypes()[0]));
            } else {
                field.setName(pmmlField.miningField.getName().getValue());
                field.setType(Arrays.asList(new String[] { "boolean" }));
            }

            dataSchema.addField(field);
        }

        for (Map.Entry<String, List<AbstractMap.Entry<String, String>>> entry : pivotValuesBySourceColumn.entrySet()) {
            String name = entry.getKey();
            UserDefinedType userType = sourceColumnTypes.get(name);
            Field field = new Field();
            field.setName(name);
            field.setType(Arrays.asList(new String[] { userType.getAvroType().toString().toLowerCase() }));
            dataSchema.addField(field);
        }
        return JsonUtils.serialize(dataSchema);
    }

    @VisibleForTesting
    final AbstractMap.Entry<String[], String> getFeaturesAndTarget(List<PmmlField> pmmlFields,
            PivotValuesLookup pivotValues) throws Exception {
        List<String> features = new ArrayList<>(pmmlFields.size());
        String event = null;
        Map<String, AbstractMap.Entry<String, List<String>>> pivotValuesByTargetColumn = pivotValues.pivotValuesByTargetColumn;
        Map<String, UserDefinedType> sourceType = pivotValues.sourceColumnToUserType;
        for (PmmlField pmmlField : pmmlFields) {
            MiningField field = pmmlField.miningField;
            String name = field.getName().getValue();
            if (field.getUsageType() == FieldUsageType.ACTIVE && !pivotValuesByTargetColumn.containsKey(name)) {
                features.add(name);
            } else if (field.getUsageType() == FieldUsageType.PREDICTED) {
                event = name;
            }
        }

        for (Map.Entry<String, UserDefinedType> entry : sourceType.entrySet()) {
            features.add(entry.getKey());
        }
        String[] f = new String[features.size()];
        features.toArray(f);
        return new AbstractMap.SimpleEntry<String[], String>(f, event);
    }

    @VisibleForTesting
    String getMetadataContents(List<PmmlField> pmmlFields, PivotValuesLookup pivotValues) throws Exception {
        ModelingMetadata metadata = new ModelingMetadata();

        List<AttributeMetadata> attrMetadata = new ArrayList<>();

        Map<String, AbstractMap.Entry<String, List<String>>> pivotValuesByTargetColumn = pivotValues.pivotValuesByTargetColumn;

        for (PmmlField pmmlField : pmmlFields) {
            MiningField field = pmmlField.miningField;
            String columnName = field.getName().getValue();

            if (pivotValuesByTargetColumn.containsKey(columnName)) {
                continue;
            }
            AttributeMetadata attrMetadatum = new AttributeMetadata();
            attrMetadatum.setColumnName(columnName);
            attrMetadatum.setApprovedUsage(
                    Arrays.asList(new String[] { ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE }));

            if (field.getOpType() == OpType.CATEGORICAL || field.getOpType() == OpType.ORDINAL) {
                attrMetadatum.setStatisticalType(ModelingMetadata.NOMINAL_STAT_TYPE);
            } else {
                attrMetadatum.setStatisticalType(ModelingMetadata.RATIO_STAT_TYPE);
            }
            attrMetadatum.setTags(Arrays.asList(new String[] { ModelingMetadata.INTERNAL_TAG }));
            attrMetadata.add(attrMetadatum);
        }
        Map<String, List<AbstractMap.Entry<String, String>>> pivotValuesBySourceColumn = pivotValues.pivotValuesBySourceColumn;
        Map<String, UserDefinedType> sourceColumnTypes = pivotValues.sourceColumnToUserType;

        for (Map.Entry<String, List<AbstractMap.Entry<String, String>>> entry : pivotValuesBySourceColumn.entrySet()) {
            String name = entry.getKey();
            UserDefinedType userType = sourceColumnTypes.get(name);
            AttributeMetadata attrMetadatum = new AttributeMetadata();
            attrMetadatum.setColumnName(name);
            attrMetadatum.setApprovedUsage(
                    Arrays.asList(new String[] { ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE }));
            // pivot columns are always categorical
            attrMetadatum.setStatisticalType(ModelingMetadata.NOMINAL_STAT_TYPE);
            attrMetadatum.setTags(Arrays.asList(new String[] { ModelingMetadata.INTERNAL_TAG }));

            // build extension for pivot values
            List<Map<String, ?>> pValues = new ArrayList<>();

            for (Map.Entry<String, String> e : entry.getValue()) {
                Map<String, Object> values = new HashMap<>();
                values.put("PivotColumn", e.getKey());
                values.put("PivotValue", userType.cast(e.getValue()));
                pValues.add(values);
            }

            List<KV> extensions = new ArrayList<>();
            extensions.add(new KV("PivotValues", pValues));
            extensions.add(new KV("DataType", userType.getAvroType()));

            attrMetadatum.setExtensions(extensions);
            attrMetadata.add(attrMetadatum);
        }

        metadata.setAttributeMetadata(attrMetadata);

        return JsonUtils.serialize(metadata);
    }

    @VisibleForTesting
    String getDataCompositionContents(List<PmmlField> pmmlFields, PivotValuesLookup pivotValues) throws Exception {
        DataComposition datacomposition = new DataComposition();
        Map<String, AbstractMap.Entry<String, List<String>>> pivotValuesByTargetColumn = pivotValues.pivotValuesByTargetColumn;
        Map<String, UserDefinedType> sourceType = pivotValues.sourceColumnToUserType;
        Map<String, FieldSchema> fields = new HashMap<>();
        for (PmmlField pmmlField : pmmlFields) {
            if (pmmlField.dataField == null) {
                continue;
            }

            MiningField field = pmmlField.miningField;
            String columnName = field.getName().getValue();

            if (pivotValuesByTargetColumn.containsKey(columnName)) {
                continue;
            }
            FieldSchema fieldSchema = new FieldSchema();
            fieldSchema.source = FieldSource.REQUEST;

            fieldSchema.type = FieldType.getFromPmmlType(pmmlField.dataField.getDataType());
            fieldSchema.interpretation = FieldInterpretation.Feature;
            fields.put(columnName, fieldSchema);
        }

        for (Map.Entry<String, UserDefinedType> columnToPivot : sourceType.entrySet()) {
            FieldSchema fieldSchema = new FieldSchema();
            fieldSchema.source = FieldSource.REQUEST;
            fieldSchema.type = columnToPivot.getValue().getFieldType();
            fieldSchema.interpretation = FieldInterpretation.Feature;
            fields.put(columnToPivot.getKey(), fieldSchema);

        }
        datacomposition.fields = fields;
        return JsonUtils.serialize(datacomposition);
    }

}
