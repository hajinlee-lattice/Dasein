package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFormat;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonMRTestUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonMRUtils;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.algorithm.AggregationAlgorithm;

public class FileAggregatorUnitTestNG {

    private String baseDir;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        String path = "com/latticeengines/dataplatform/runtime/mapreduce/Q_EVENT_NUTANIX";
        baseDir = ClassLoader.getSystemResource(path).getPath();
    }

    @Test(groups = "unit")
    public void testProfileAvroAggregator() throws Exception {
        String inputDir = baseDir + "/profile/parts";
        String comparePath = baseDir + "/profile/" + FileAggregator.PROFILE_AVRO;
        List<String> paths = getFilesForDir(inputDir, HdfsFileFormat.AVRO_FILE);

        ProfileAvroAggregator aggregator = new ProfileAvroAggregator();
        aggregator.aggregateToLocal(paths);

        List<GenericRecord> aggregatedRecords = AvroUtils.readFromLocalFile(FileAggregator.PROFILE_AVRO);
        List<GenericRecord> originalRecords = AvroUtils.readFromLocalFile(comparePath);

        assertEquals(aggregatedRecords.size(), originalRecords.size());
        FileUtils.deleteQuietly(new File(FileAggregator.PROFILE_AVRO));
    }

    @Test(groups = "unit")
    public void testDiagnosticsJsonAggregator() throws Exception {
        String inputDir = baseDir + "/diagnostics/parts";
        String comparePath = baseDir + "/diagnostics/" + FileAggregator.DIAGNOSTICS_JSON;
        List<String> paths = getFilesForDir(inputDir, HdfsFileFormat.JSON_FILE);

        DiagnosticsJsonAggregator aggregator = new DiagnosticsJsonAggregator();
        aggregator.aggregateToLocal(paths);

        String aggregatedDiagnostics = FileUtils.readFileToString(new File(FileAggregator.DIAGNOSTICS_JSON));
        String original = FileUtils.readFileToString(new File(comparePath));
        String orginalDiagnostics = new ObjectMapper().readTree(original).toString();

        assertEquals(aggregatedDiagnostics.length(), orginalDiagnostics.length());
        FileUtils.deleteQuietly(new File(FileAggregator.DIAGNOSTICS_JSON));
    }

    @Test(groups = "unit")
    public void testModelPickleAggregator() throws Exception {
        Configuration config = new Configuration();
        Classifier classifier = PythonMRTestUtils.setupDummyClassifier();
        String metadata = JsonUtils.serialize(classifier);
        config.set(PythonContainerProperty.METADATA_CONTENTS.name(), metadata);
        config.set(MapReduceProperty.VERSION.name(), "");

        ModelPickleAggregator aggregator = new ModelPickleAggregator();
        try {
            aggregator.aggregate(new ArrayList<String>(), config);
        } catch (Exception e) {
            // ignore invoker exception
        }
        String metadataNew = FileUtils.readFileToString(new File(PythonMRUtils.METADATA_JSON_PATH));
        Classifier classifierNew = JsonUtils.deserialize(metadataNew, Classifier.class);

        assertNotEquals(metadataNew, metadata);
        assertEquals(classifierNew.getPythonScriptHdfsPath().replaceAll("//", "/"), new AggregationAlgorithm().getScript());
        FileUtils.deleteQuietly(new File(PythonMRUtils.METADATA_JSON_PATH));
    }

    public List<String> getFilesForDir(String parentDir, final String regex) {
        File[] files = new File(parentDir).listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                Pattern p = Pattern.compile(regex);
                Matcher matcher = p.matcher(name.toString());
                return matcher.matches();
            }

        });

        List<String> paths = new ArrayList<String>();
        for (File file : files) {
            paths.add(file.getPath());
        }
        return paths;
    }
}
