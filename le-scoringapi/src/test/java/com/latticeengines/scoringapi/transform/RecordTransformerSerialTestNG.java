package com.latticeengines.scoringapi.transform;

import java.io.BufferedReader;
import java.io.File;
import java.io.Reader;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
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

import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.common.exposed.modeling.ModelExtractor;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.scoringapi.exposed.ScoreType;
import com.latticeengines.scoringapi.exposed.model.ModelEvaluator;
import com.latticeengines.scoringapi.exposed.model.impl.DefaultModelEvaluator;
import com.latticeengines.scoringapi.functionalframework.RecordTransformerTestMetadata;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;

/**
 * This test provides the ability to run all the tenants. This entire test is
 * data-driven. It expects that in
 * src/test/resources/com/latticeengines/scoringapi/transform/tenants exists a
 * number of folders. The folder name is considered to be the tenant name.
 * Underneath this folder, the test expects a number of files:
 *
 * 1. metadata.json file - must contain key_column value that represents the key column of the source training set 
 * 2. datacomposition.json file - file generated by the modeling service that represents the real-time transform document 
 * 3. datatoscore.avro file - the test set during modeling 
 * 4. model.json file - the model file generated during modeling 
 * 5. rfpmml.xml file - the pmml file generated during modeling 
 * 6. scored.txt file - the scored test set that this test compares the PMML score against 
 * 7. scorederivation.json - file generated by the modeling service used by the PMML evaluation engine
 *
 * @author zandrogonzalez
 *
 */
public class RecordTransformerSerialTestNG extends ScoringApiFunctionalTestNGBase {

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
            RecordTransformerTestMetadata metadata = JsonUtils.deserialize(
                    FileUtils.readFileToString(//
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

        if (tenantToScore == null || tenantToScore.equals(tenantName)) {
            return false;
        }
        return true;
    }

    @Test(groups = "functional", dataProvider = "tenants", enabled = true)
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

        System.out.println("Processing tenant " + tenantName);
        transformAndScore(dataToScorePath, dataComposition.transforms, pmmlXmlPath, scoreDerivation, expectedScoresPath,
                keyColumn);
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
        ModelEvaluator pmmlEvaluator = new DefaultModelEvaluator(pmmlReader);
        Schema schema = AvroUtils.getSchema(config, new Path(avroFile));
        Map<Double, Double> expectedScores = getExpectedScores(expectedScoresPath);
        int i = 0;

        long totalFastTransformTime = 0;
        long totalTransformTime = 0;
        long totalEvaluationTime = 0;
        long time0 = System.currentTimeMillis();
        int transformDifferences = 0;
        int errors = 0;
        List<Map.Entry<Double, Double>> errorKeys = new ArrayList<>();

        @SuppressWarnings("unused")
        JythonEngine engine = new JythonEngine(modelExtractionDir.getAbsolutePath());
        
        for (GenericRecord record : reader) {
            Object recId = record.get(keyColumn);
            Double recIdAsDouble = null;
            if (recId instanceof Long) {
                recIdAsDouble = ((Long) recId).doubleValue();
            } else if (recId instanceof Integer) {
                recIdAsDouble = ((Integer) recId).doubleValue();
            } else if (recId instanceof Utf8) {
                recIdAsDouble = Double.valueOf(((Utf8) recId).toString());
            }

            Map<String, Object> recordAsMap = new HashMap<>();
            for (Field f : schema.getFields()) {
                Object value = record.get(f.name());
                if (value instanceof Utf8) {
                    value = ((Utf8) value).toString();
                }
                recordAsMap.put(f.name(), value);
            }
            long time9 = System.currentTimeMillis();
            
//            Map<String, Object> transformedFast = recordTransformer.transformJython(engine, transforms, recordAsMap);
//            Map<String, Object> transformedFast = recordTransformer.transformJython(modelExtractionDir.getAbsolutePath(), transforms, recordAsMap);
            Map<String, Object> transformedFast = recordTransformer.transform(modelExtractionDir.getAbsolutePath(), transforms, recordAsMap);
            long time10 = System.currentTimeMillis();
            totalFastTransformTime += (time10 - time9);

            try {
                Map<ScoreType, Object> evaluationFast = pmmlEvaluator.evaluate(transformedFast, derivation);
                totalEvaluationTime += (System.currentTimeMillis() - time10);
                double expectedScore = expectedScores.get(recIdAsDouble);
                double scoreFast = (double) evaluationFast.get(ScoreType.PROBABILITY_OR_VALUE);

                if (Math.abs(expectedScore - scoreFast) > 0.000001) {
                    errors++;
                    errorKeys.add(new AbstractMap.SimpleEntry<Double, Double>(recIdAsDouble,
                            Math.abs(expectedScore - scoreFast)));
                    System.out.println(
                            String.format("Difference for Record id %f has diff of %f value %f, expected is %f",
                                    recIdAsDouble, Math.abs(expectedScore - scoreFast), scoreFast, expectedScore));
                }
            } catch (Exception e) {
                errors++;
                System.out.println(e);
                errorKeys.add(new AbstractMap.SimpleEntry<Double, Double>(recIdAsDouble, -1.0));
            }

            if (i % 1000 == 1) {
                System.out.println("At record " + i + " in " + (System.currentTimeMillis() - time0) + " ms.");
                System.out.println(String.format("Average transform time per record = %.3f",
                        (double) totalTransformTime / (double) i));
                System.out.println(String.format("Average fast transform time per record = %.3f",
                        (double) totalFastTransformTime / (double) i));
                System.out.println(String.format("Differences fast transform vs transform = %d", transformDifferences));
            }
            i++;
        }

        System.out.println(
                String.format("Average transform time per record = %.3f", (double) totalTransformTime / (double) i));
        System.out.println(String.format("Average fast transform time per record = %.3f",
                (double) totalFastTransformTime / (double) i));
        System.out.println(
                String.format("Average evaluation time per record = %.3f", (double) totalEvaluationTime / (double) i));
        System.out.println("Number of errors = " + errors);
        for (Map.Entry<Double, Double> entry : errorKeys) {
            System.out.println("Record id = " + entry.getKey() + " value = " + entry.getValue());
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
