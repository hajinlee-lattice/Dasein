package com.latticeengines.skald.scoringtest;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.skald.CombinationElement;
import com.latticeengines.skald.exposed.ScoreType;

public class TestDirectoryReader {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private final String path;

    public TestDirectoryReader(String path) {
        this.path = path;
    }

    public List<TestDefinition> read() throws Exception {
        List<TestDefinition> definitions = new ArrayList<TestDefinition>();

        File parentDirectory = new File(path);
        File[] testDirectories = parentDirectory.listFiles();
        for (File directory : testDirectories) {
            if (directory.isDirectory()) {
                try {
                    definitions.add(readDefinition(directory));
                } catch (Exception e) {
                    log.error("An error occurred reading the files in directory " + directory.getAbsolutePath(), e);
                    throw e;
                }
            }
        }

        return definitions;
    }

    private TestDefinition readDefinition(File testDirectory) throws Exception {
        TestDefinition definition = new TestDefinition();
        definition.testName = testDirectory.getName();
        definition.combination = readCombination(testDirectory);
        definition.records = readRecords(testDirectory, definition.combination);
        definition.scores = readScores(testDirectory);
        definition.modelsPMML = readModels(testDirectory, definition.combination);

        if (definition.records.size() != definition.scores.size()) {
            throw new RuntimeException("Records and scores must be the same size.  This fails to be the case for test "
                    + definition.testName + ". #Scores = " + definition.scores.size() + " and #Records = "
                    + definition.records.size());
        }

        return definition;
    }

    private List<Map<String, Object>> readRecords(File testDirectory, List<CombinationElement> combination)
            throws Exception {
        Map<String, FieldSchema> map = new HashMap<String, FieldSchema>();
        for (CombinationElement element : combination) {
            for (Entry<String, FieldSchema> pair : element.data.fields.entrySet()) {
                String fieldname = pair.getKey();
                FieldSchema schema = pair.getValue();
                if (map.containsKey(fieldname)) {
                    FieldSchema existing = map.get(fieldname);
                    if (!existing.equals(schema)) {
                        throw new RuntimeException("Fields with the same name must have the same schema.  Field "
                                + fieldname + " is specified with the following two different schemas: " + schema
                                + " and schema " + existing);
                    }
                }
                map.put(fieldname, schema);
            }
        }
        RecordReader reader = new RecordReader(Paths.get(testDirectory.getAbsolutePath(), "records.csv").toString(),
                map);
        return reader.read();
    }

    private List<Map<ScoreType, Object>> readScores(File testDirectory) throws Exception {
        ScoreReader reader = new ScoreReader(Paths.get(testDirectory.getAbsolutePath(), "scores.csv").toString());
        return reader.read();
    }

    private List<CombinationElement> readCombination(File testDirectory) {
        Path combinationPath = Paths.get(testDirectory.getAbsolutePath(), "combination.json");
        try {
            FileReader reader = new FileReader(combinationPath.toString());
            CombinationElement[] combination = new ObjectMapper().readValue(reader, CombinationElement[].class);
            return Arrays.asList(combination);
        } catch (Exception e) {
            throw new RuntimeException("Unable to read combination file at " + combinationPath, e);
        }
    }

    private Map<String, String> readModels(File testDirectory, List<CombinationElement> combination) throws Exception {
        Map<String, String> modelsPMML = new HashMap<String, String>();
        for (CombinationElement element : combination) {
            Path modelPath = Paths.get(testDirectory.getAbsolutePath(), element.model.name + ".pmml");
            String text;
            try {
                byte[] buffer = Files.readAllBytes(modelPath);
                text = new String(buffer, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read PMML file " + modelPath, e);
            }
            modelsPMML.put(element.model.name, text);
        }
        return modelsPMML;
    }

}
