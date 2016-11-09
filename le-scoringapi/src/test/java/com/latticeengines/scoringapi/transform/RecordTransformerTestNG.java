package com.latticeengines.scoringapi.transform;

import java.io.BufferedReader;
import java.io.File;
import java.io.Reader;
import java.net.URL;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
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
 * 1. metadata.json file - must contain key_column value that represents the key
 * column of the source training set 2. datacomposition.json file - file
 * generated by the modeling service that represents the real-time transform
 * document 3. datatoscore.avro file - the test set during modeling 4.
 * model.json file - the model file generated during modeling 5. rfpmml.xml file
 * - the pmml file generated during modeling 6. scored.txt file - the scored
 * test set that this test compares the PMML score against 7.
 * scorederivation.json - file generated by the modeling service used by the
 * PMML evaluation engine
 *
 * Lastly, this is the parallel version of the test that spawns multiple threads
 * to ensure that RecordTransformer and ModelEvaluator can run concurrently
 * without impacting scoring correctness.
 * 
 * @author zandrogonzalez
 *
 */
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

        BlockingQueue<GenericRecord> inputQueue = new PriorityBlockingQueue<>();
        BlockingQueue<QueueEntry<Double, Double>> outputQueue = new PriorityBlockingQueue<>();
        BlockingQueue<QueueEntry<Double, Double>> errorQueue = new PriorityBlockingQueue<>();

        ExecutorService transformService = Executors.newFixedThreadPool(10);

        transformService.execute(new RecordTransformerCallable(schema, inputQueue, outputQueue, errorQueue, //
                keyColumn, modelExtractionDir.getAbsolutePath(), transforms, derivation, recordTransformer, //
                pmmlEvaluator, expectedScores));

        for (GenericRecord record : reader) {
            inputQueue.put(record);
            i++;
        }

        while (outputQueue.size() + errorQueue.size() != i) {
            System.out.println("Output queue size = " + outputQueue.size());
            System.out.println("Error queue size = " + errorQueue.size());
            Thread.sleep(10000L);
        }
        System.out.println("Number of errors = " + errorQueue.size());

        for (QueueEntry<Double, Double> entry : errorQueue) {
            System.out.println("Record id = " + entry.getKey() + " value = " + entry.getValue());
        }
        Assert.assertTrue(errorQueue.size() < 7, "Number of errors " + errorQueue.size() + " is greater than threshold 7.");
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

    private static class RecordTransformerCallable implements Callable<Void>, Runnable {

        private final BlockingQueue<GenericRecord> inputQueue;
        private final BlockingQueue<QueueEntry<Double, Double>> outputQueue;
        private final BlockingQueue<QueueEntry<Double, Double>> errorQueue;
        private final String modelPath;
        private final String keyColumn;
        private final List<TransformDefinition> transforms;
        private final ScoreDerivation derivation;
        private final RecordTransformer recordTransformer;
        private final ModelEvaluator pmmlEvaluator;
        private Map<Double, Double> expectedScores;
        private Schema schema;

        public RecordTransformerCallable(Schema schema, //
                BlockingQueue<GenericRecord> inputQueue, //
                BlockingQueue<QueueEntry<Double, Double>> outputQueue, //
                BlockingQueue<QueueEntry<Double, Double>> errorQueue, //
                String keyColumn, //
                String modelPath, //
                List<TransformDefinition> transforms, //
                ScoreDerivation derivation, //
                RecordTransformer recordTransformer, //
                ModelEvaluator pmmlEvaluator, //
                Map<Double, Double> expectedScores) {
            this.schema = schema;
            this.inputQueue = inputQueue;
            this.outputQueue = outputQueue;
            this.errorQueue = errorQueue;
            this.modelPath = modelPath;
            this.keyColumn = keyColumn;
            this.transforms = transforms;
            this.derivation = derivation;
            this.recordTransformer = recordTransformer;
            this.pmmlEvaluator = pmmlEvaluator;
            this.expectedScores = expectedScores;
        }

        private Map.Entry<Double, Map<String, Object>> getRecord(GenericRecord record) throws Exception {
            Object recId = record.get(keyColumn);
            Double recIdAsDouble = null;
            if (recId instanceof Long) {
                recIdAsDouble = ((Long) recId).doubleValue();
            } else if (recId instanceof Integer) {
                recIdAsDouble = ((Integer) recId).doubleValue();
            } else {
                recIdAsDouble = Double.valueOf(recId.toString());
            }

            Map<String, Object> recordAsMap = new HashMap<>();
            for (Field f : schema.getFields()) {
                Object value = record.get(f.name());
                if (value instanceof Utf8) {
                    value = ((Utf8) value).toString();
                }
                recordAsMap.put(f.name(), value);
            }
            return new AbstractMap.SimpleEntry<>(recIdAsDouble, recordAsMap);
        }

        @Override
        public Void call() throws Exception {
            while (true) {
                GenericRecord r = inputQueue.take();
                if (r == null) {
                    continue;
                }

                Map.Entry<Double, Map<String, Object>> record = getRecord(r);
                if (record == null) {
                    continue;
                }
                Double key = record.getKey();
                try {
                    Map<String, Object> transformedFast = recordTransformer.transform(modelPath, transforms,
                            record.getValue());

                    Map<ScoreType, Object> evaluationFast = null;
                    evaluationFast = pmmlEvaluator.evaluate(transformedFast, derivation);

                    Double expectedScore = expectedScores.get(record.getKey());
                    Double scoreFast = (double) evaluationFast.get(ScoreType.PROBABILITY);

                    if (Math.abs(expectedScore - scoreFast) > 0.000001) {
                        System.out.println(String.format("Record id %f has value %f, expected is %f", //
                                key, scoreFast, expectedScore));
                        System.out.println("Difference," + Math.abs(expectedScore - scoreFast));

                        errorQueue.put(new QueueEntry<Double, Double>(key, Math.abs(expectedScore - scoreFast)));
                    } else {
                        outputQueue.put(new QueueEntry<Double, Double>(key, scoreFast));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errorQueue.put(new QueueEntry<Double, Double>(key, -1.0));
                }
            }
        }

        @Override
        public void run() {
            try {
                call();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    static class QueueEntry<K extends Comparable<K>, V extends Comparable<V>> extends AbstractMap.SimpleEntry<K, V>
            implements Comparable<QueueEntry<K, V>> {

        private static final long serialVersionUID = 1L;

        public QueueEntry(K key, V value) {
            super(key, value);
        }

        public QueueEntry(Entry<? extends K, ? extends V> entry) {
            super(entry);
        }

        @Override
        public int compareTo(QueueEntry<K, V> o) {
            return o.getKey().compareTo(getKey());
        }

    }
}
