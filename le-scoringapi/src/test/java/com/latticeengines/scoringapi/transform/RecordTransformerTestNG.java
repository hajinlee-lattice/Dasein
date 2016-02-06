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
import com.latticeengines.scoringapi.functionalframework.RecordTransformerTestMetadata;
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
    public Object[][] getTenants() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/scoringapi/transform/tenants");
        File transformDir = new File(url.getFile());
        
        List<File> tenantList = Arrays.asList(transformDir.listFiles());
        Object[][] tenants = new Object[tenantList.size()][2];
        int i = 0;
        for (File tenant : tenantList) {
            tenants[i][0] = tenant.getAbsolutePath();
            RecordTransformerTestMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(//
                    new File(tenant.getAbsolutePath() + "/metadata.json")), //
                    RecordTransformerTestMetadata.class);
            tenants[i][1] = metadata.keyColumn;
            i++;
        }
        
        return tenants;
    }
    
    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {
        if (modelExtractionDir != null) {
            FileUtils.deleteDirectory(modelExtractionDir);
        }
    }
    
    private boolean skip(String tenantName) {
        String tenantToScore = System.getProperty("TENANT");
        
        if (tenantToScore != null && tenantToScore.equals(tenantName)) {
            return false;
        }
        return true;
    }

    @Test(groups = "functional", dataProvider = "tenants")
    public void transform(String tenantPath, String keyColumn) throws Exception {
        String modelFilePath = tenantPath + "/model.json";
        String dataToScorePath = tenantPath + "/datatoscore.avro";
        String dataCompositionPath = tenantPath + "/datacomposition.json";
        String scoreDerivationPath = tenantPath + "/scorederivation.json";
        String pmmlXmlPath = tenantPath + "/rfpmml.xml";
        String expectedScoresPath = tenantPath + "/scored.txt";
        String tenantName = new File(tenantPath).getName();
        
        if (skip(tenantName)) {
            return;
        }
        
        modelExtractionDir = new File(String.format("/tmp/%s/", tenantName));
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
        int i = 0;
        
        long totalTransformTime = 0;
        long totalEvaluationTime = 0;
        long time0 = System.currentTimeMillis();
        for (GenericRecord record : reader) {
            Object recId = record.get(keyColumn);
            Double recIdAsDouble = null;
            if (recId instanceof Long) {
                recIdAsDouble = ((Long) recId).doubleValue();
            } else if (recId instanceof Integer) {
                recIdAsDouble = (double) recId;
            } else {
                throw new RuntimeException("Key not instance of Long or Integer.");
            }
        
            
            Map<String, Object> recordAsMap = new HashMap<>();
            for (Field f : schema.getFields()) {
                Object value = record.get(f.name());
                if (value instanceof Utf8) {
                    value = ((Utf8) value).toString();
                }
                recordAsMap.put(f.name(), value);
            }
            long time1 = System.currentTimeMillis();
            Map<String, Object> transformed = recordTransformer.transform(modelExtractionDir.getAbsolutePath(), transforms, recordAsMap);
            long time2 = System.currentTimeMillis();
            totalTransformTime += (time2 - time1);
            
            Map<ScoreType, Object> evaluation = pmmlEvaluator.evaluate(transformed, derivation);
            totalEvaluationTime += (System.currentTimeMillis() - time2);
            double expectedScore = expectedScores.get(recIdAsDouble);
            double score = (double) evaluation.get(ScoreType.PROBABILITY);
            if (Math.abs(expectedScore - score) > 0.0000001) {
                System.out.println(String.format("Record id %d has value %f, expected is %f", recId, score, expectedScore));
            }
            if (i % 1000 == 0) {
                System.out.println("At record " + i + " in " + (System.currentTimeMillis() - time0) + " ms.");
            }
            i++;
        }
        
        System.out.println(String.format("Average transform time per record = %.3f", (double) totalTransformTime/ (double) i));
        System.out.println(String.format("Average evaluation time per record = %.3f", (double) totalEvaluationTime/ (double) i));
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
