package com.latticeengines.modeling.workflow.steps;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.KV;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.pmml.PmmlField;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.CreatePMMLModelConfiguration;
import com.latticeengines.domain.exposed.util.ModelingUtils;
import com.latticeengines.domain.exposed.util.PmmlModelUtils;

public class CreatePMMLModelUnitTestNG {

    private CreatePMMLModel createPMMLModel = new CreatePMMLModel();
    private Configuration yarnConfiguration;
    private CreatePMMLModelConfiguration config;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        URL pivotValuesUrl = ClassLoader
                .getSystemResource("com/latticeengines/leadprioritization/workflow/steps/createPMMLModel/pivotvalues.txt");
        URL pmmlUrl = ClassLoader
                .getSystemResource("com/latticeengines/leadprioritization/workflow/steps/createPMMLModel/rfpmml.xml");
        config = new CreatePMMLModelConfiguration();
        config.setPivotArtifactPath(pivotValuesUrl.getPath());
        config.setPmmlArtifactPath(pmmlUrl.getFile());

        ReflectionTestUtils.setField(createPMMLModel, "configuration", config);

        yarnConfiguration = new Configuration();
        yarnConfiguration.set("fs.defaultFS", "file:///");
        ReflectionTestUtils.setField(createPMMLModel, "yarnConfiguration", yarnConfiguration);
    }

    @Test(groups = "unit")
    public void getPivotValues() throws Exception {
        PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, config.getPivotArtifactPath());
        assertEquals(pivotValues.pivotValuesByTargetColumn.size(), 8);
        assertEquals(pivotValues.pivotValuesBySourceColumn.size(), 1);
        assertEquals(pivotValues.sourceColumnToUserType.size(), 1);
    }

    @Test(groups = "unit")
    public void getPmmlFields() throws Exception {
        PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, config.getPivotArtifactPath());
        InputStream inputStream = FileUtils.openInputStream(new File(config.getPmmlArtifactPath()));
        List<PmmlField> fields = PmmlModelUtils.getPmmlFields(inputStream);
        assertEquals(fields.size(), 117);
        String[] features = createPMMLModel.getFeaturesAndTarget(fields, pivotValues).getKey();

        boolean found = false;
        for (String feature : features) {
            if (feature.equals("PD_DA_JobTitle")) {
                found = true;
            }
        }

        assertTrue(found, "PD_DA_JobTitle not found.");
        assertEquals(features.length, 109);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "unit")
    public void getMetadataContents() throws Exception {
        PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, config.getPivotArtifactPath());
        InputStream inputStream = FileUtils.openInputStream(new File(config.getPmmlArtifactPath()));
        List<PmmlField> pmmlFields = PmmlModelUtils.getPmmlFields(inputStream);

        String metadataStr = createPMMLModel.getMetadataContents(pmmlFields, pivotValues);
        ModelingMetadata modelingMetadata = JsonUtils.deserialize(metadataStr, ModelingMetadata.class);
        List<AttributeMetadata> attrMetadata = modelingMetadata.getAttributeMetadata();

        for (AttributeMetadata attrMetadatum : attrMetadata) {
            if (attrMetadatum.getColumnName().equals("PD_DA_JobTitle")) {
                KV kv = attrMetadatum.getExtensions().get(0);
                assertEquals(((List) kv.getValue()).size(), 9);
            }

        }
    }

    @Test(groups = "unit")
    public void getDataCompositionContents() throws Exception {
        PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, config.getPivotArtifactPath());
        InputStream inputStream = FileUtils.openInputStream(new File(config.getPmmlArtifactPath()));
        List<PmmlField> pmmlFields = PmmlModelUtils.getPmmlFields(inputStream);
        String datacompositionStr = createPMMLModel.getDataCompositionContents(pmmlFields, pivotValues);
        DataComposition datacomposition = JsonUtils.deserialize(datacompositionStr, DataComposition.class);
        assertNotNull(datacomposition);
        FieldSchema fieldSchema = datacomposition.fields.get("PD_DA_JobTitle");
        assertNotNull(fieldSchema);
        assertEquals(fieldSchema.type, FieldType.STRING);
    }

    @Test(groups = "unit")
    public void getAvroSchema() throws Exception {
        PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, config.getPivotArtifactPath());
        InputStream inputStream = FileUtils.openInputStream(new File(config.getPmmlArtifactPath()));
        List<PmmlField> pmmlFields = PmmlModelUtils.getPmmlFields(inputStream);
        String avroSchemaStr = createPMMLModel.getAvroSchema(pmmlFields, pivotValues);
        Schema schema = new Schema.Parser().parse(avroSchemaStr);
        assertNotNull(schema);

        boolean foundPivotColumn = false;
        boolean foundEvent = false;
        for (Field f : schema.getFields()) {

            if (f.name().equals("PD_DA_JobTitle")) {
                foundPivotColumn = true;
            }

            if (f.name().equals("P1_Event")) {
                foundEvent = true;
            }
        }

        assertTrue(foundPivotColumn, "PD_DA_JobTitle not found.");
        assertTrue(foundEvent, "P1_Event not found.");
    }

    @Test(groups = "unit")
    public void getFeaturesAndTarget() throws Exception {
        PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, config.getPivotArtifactPath());
        InputStream inputStream = FileUtils.openInputStream(new File(config.getPmmlArtifactPath()));
        List<PmmlField> pmmlFields = PmmlModelUtils.getPmmlFields(inputStream);
        Map.Entry<String[], String> featuresAndTarget = createPMMLModel.getFeaturesAndTarget(pmmlFields, pivotValues);
        assertNotNull(featuresAndTarget.getValue());
    }

    @Test(groups = "unit")
    public void getMetadataArtifacts() throws Exception {
        Map<ArtifactType, String> result = createPMMLModel.getMetadataArtifacts();
        assertNotNull(result.get(ArtifactType.PMML));
    }
}
