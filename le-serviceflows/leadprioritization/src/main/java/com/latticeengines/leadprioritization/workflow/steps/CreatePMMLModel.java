package com.latticeengines.leadprioritization.workflow.steps;

import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.sax.SAXSource;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.Model;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

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
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.leadprioritization.workflow.steps.pmml.PMMLModelingServiceExecutor;
import com.latticeengines.leadprioritization.workflow.steps.pmml.PmmlField;
import com.latticeengines.leadprioritization.workflow.steps.pmml.SkipFilter;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.serviceflows.workflow.modeling.PivotValuesLookup;
import com.latticeengines.serviceflows.workflow.util.ModelingUtils;

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
            executionContext.put(MODEL_APP_IDS, JsonUtils.serialize(modelApplicationIdToEventColumn));
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new LedpException(LedpCode.LEDP_28019, new String[] { configuration.getPmmlArtifactPath() });
        }
    }

    protected PMMLModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            CreatePMMLModelConfiguration modelStepConfiguration) throws Exception {
        List<PmmlField> pmmlFields = getPmmlFields();
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
                .yarnConfiguration(yarnConfiguration);

        return bldr;
    }

    private Map<ArtifactType, String> getMetadataArtifacts() {
        Map<ArtifactType, String> metadataArtifacts = new HashMap<>();
        metadataArtifacts.put(ArtifactType.PMML, configuration.getPmmlArtifactPath());
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
                FieldType fieldType = getFieldType(pmmlField.dataField.getDataType());
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
            attrMetadatum.setApprovedUsage(Arrays
                    .asList(new String[] { ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE }));

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
            attrMetadatum.setApprovedUsage(Arrays
                    .asList(new String[] { ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE }));
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

            fieldSchema.type = getFieldType(pmmlField.dataField.getDataType());
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

    // TODO: temporary mapping to avoid adding the new PMML dependency in
    // le-domain
    // Fix this in FieldType when JPMML version is updated across the entire
    // product
    // and the RandomForest change has been ported over from our version to the
    // new version of JPMML
    private FieldType getFieldType(DataType pmmlDataType) {
        switch (pmmlDataType.toString()) {
        case "BOOLEAN":
            return FieldType.BOOLEAN;
        case "STRING":
            return FieldType.STRING;
        case "INTEGER":
            return FieldType.INTEGER;
        case "DOUBLE":
        case "FLOAT":
            return FieldType.FLOAT;
        default:
            return FieldType.STRING;
        }
    }

    @VisibleForTesting
    final List<PmmlField> getPmmlFields() throws Exception {
        String pmmlPath = configuration.getPmmlArtifactPath();

        InputStream pmmlStream = HdfsUtils.getInputStream(yarnConfiguration, pmmlPath);
        InputSource source = new InputSource(pmmlStream);

        XMLReader reader = XMLReaderFactory.createXMLReader();
        XMLFilter importFilter = new ImportFilter(reader);
        XMLFilter skipSegmentationFilter = new SkipFilter(reader, "Segmentation");
        skipSegmentationFilter.setParent(importFilter);
        XMLFilter skipExtensionFilter = new SkipFilter(reader, "Extension");
        skipExtensionFilter.setParent(skipSegmentationFilter);
        SAXSource transformedSource = new SAXSource(skipExtensionFilter, source);

        PMML pmml = JAXBUtil.unmarshalPMML(transformedSource);
        Map<String, DataField> dataFields = getDataFields(pmml);

        Model model = pmml.getModels().get(0);
        List<MiningField> miningFields = model.getMiningSchema().getMiningFields();

        List<PmmlField> pmmlFields = new ArrayList<>();
        for (MiningField miningField : miningFields) {
            DataField f = dataFields.get(miningField.getName().getValue());

            if (f == null && miningField.getUsageType() != FieldUsageType.PREDICTED) {
                continue;
            }
            pmmlFields.add(new PmmlField(miningField, f));
        }

        return pmmlFields;
    }

    private Map<String, DataField> getDataFields(PMML pmml) {
        DataDictionary dataDictionary = pmml.getDataDictionary();
        List<DataField> dataFields = dataDictionary.getDataFields();
        Map<String, DataField> map = new HashMap<>();
        for (DataField dataField : dataFields) {
            map.put(dataField.getName().getValue(), dataField);
        }
        return map;
    }

}
