package com.latticeengines.scoringapi.transform;

import java.io.BufferedReader;
import java.io.File;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.modeling.ModelExtractor;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;
import com.latticeengines.scoringapi.model.ModelEvaluator;
import com.latticeengines.scoringapi.unused.ScoreType;

public class RecordTransformerTestNG extends ScoringApiFunctionalTestNGBase {
    
    private File modelExtractionDir;
    
    @Autowired
    private RecordTransformer recordTransformer;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }
    
    @DataProvider(name = "tenants")
    public Object[][] getTenants() {
        URL url = ClassLoader.getSystemResource("com/latticeengines/scoringapi/transform/tenants");
        File transformDir = new File(url.getFile());
        
        List<File> tenantList = Arrays.asList(transformDir.listFiles());
        Object[][] tenants = new Object[tenantList.size()][2];
        
        int i = 0;
        for (File tenant : tenantList) {
            tenants[i][0] = tenant.getAbsolutePath();
            tenants[i][1] = "Nutanix_EventTable_Clean";
        }
        
        return tenants;
    }
    
    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(modelExtractionDir);
    }

    @Test(groups = "functional", dataProvider = "tenants")
    public void transform(String tenantPath, String keyColumn) throws Exception {
        String modelFilePath = tenantPath + "/model.json";
        String dataToScorePath = tenantPath + "/datatoscore.avro";
        String dataCompositionPath = tenantPath + "/datacomposition.json";
        String scoreDerivationPath = tenantPath + "/scorederivation.json";
        String pmmlXmlPath = tenantPath + "/rfpmml.xml";
        String expectedScoresPath = tenantPath + "/scored.txt";
        modelExtractionDir = new File(String.format("/tmp/%s/", new File(tenantPath).getName()));
        modelExtractionDir.mkdir();
        
        new ModelExtractor().extractModelArtifacts(modelFilePath, modelExtractionDir.getAbsolutePath());
        
        DataComposition dataComposition = JsonUtils.deserialize( //
                FileUtils.readFileToString(new File(dataCompositionPath)), DataComposition.class);
        ScoreDerivation scoreDerivation = JsonUtils.deserialize(FileUtils.readFileToString( //
                new File(scoreDerivationPath)), ScoreDerivation.class);
        
        transformAndScore(dataToScorePath, dataComposition.transforms, pmmlXmlPath, scoreDerivation, expectedScoresPath, keyColumn);
    }
    
    private void transformAndScore(String avroFile, //
            List<TransformDefinition> transforms, //
            String pmmlXmlPath, //
            ScoreDerivation derivation, //
            String expectedScoresPath, //
            String keyColumn) throws Exception {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", "file:///");
        FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config, new Path(avroFile));
        Reader pmmlReader = new java.io.FileReader(pmmlXmlPath);
        ModelEvaluator pmmlEvaluator = new ModelEvaluator(pmmlReader);
        Schema schema = AvroUtils.getSchema(config, new Path(avroFile));
        Map<Double, Double> expectedScores = getExpectedScores(expectedScoresPath);
        for (GenericRecord record : reader) {
            Integer recId = (Integer) record.get(keyColumn);
            
            Map<String, Object> recordAsMap = new HashMap<>();
            for (Field f : schema.getFields()) {
                Object value = record.get(f.name());
                if (value instanceof Utf8) {
                    value = ((Utf8) value).toString();
                }
                recordAsMap.put(f.name(), value);
            }
            Map<String, Object> transformed = recordTransformer.transform(modelExtractionDir.getAbsolutePath(), transforms, recordAsMap);

            Map<ScoreType, Object> evaluation = pmmlEvaluator.evaluate(transformed, derivation);
            double expectedScore = expectedScores.get((double) recId);
            double score = (double) evaluation.get(ScoreType.PROBABILITY);
            if (Math.abs(expectedScore - score) > 0.0000001) {
                System.out.println(String.format("Record id %d has value %f, expected is %f", recId, score, expectedScore));
            }
        }
    }
    
    private Map<Double, Double> getExpectedScores(String expectedScoresPath) throws Exception {
        Map<Double, Double> scores = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new java.io.FileReader(expectedScoresPath))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                scores.put(Double.valueOf(tokens[0]), Double.valueOf(tokens[1]));
            }
        }
        return scores;
    }
}
