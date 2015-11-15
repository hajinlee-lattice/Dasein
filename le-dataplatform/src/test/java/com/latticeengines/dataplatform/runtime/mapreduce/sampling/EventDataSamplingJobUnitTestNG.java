package com.latticeengines.dataplatform.runtime.mapreduce.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.runtime.mapreduce.sampling.EventDataSamplingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class EventDataSamplingJobUnitTestNG {

    private String inputDir = null;
    private String outputDir = null;
    private SamplingConfiguration samplingConfig = null;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        URL inputUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/runtime/mapreduce/DELL_EVENT_TABLE");
        inputDir = inputUrl.getPath();
        outputDir = inputDir + "/samples";
        FileUtils.deleteDirectory(new File(outputDir));
        samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
    }

    private File[] getAvroFilesForDir(String parentDir) {
        return new File(parentDir).listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".avro");
            }

        });
    }

    @Test(groups = "unit")
    public void run() throws Exception {
        File[] inputAvroFiles = getAvroFilesForDir(inputDir);
        Map<Object, Integer> histogram = new HashMap<Object, Integer>();
        int numRows = 0;
        for (File avroFile : inputAvroFiles) {
            Pair<Map<Object, Integer>, Integer> retval = computeDistribution(avroFile.getAbsolutePath(), "Event_Latitude_Customer", histogram);
            numRows += retval.value();
        }


        int res = ToolRunner.run(new EventDataSamplingJob(new Configuration()), new String[] { inputDir, outputDir, samplingConfig.toString(), LedpQueueAssigner.getModelingQueueNameForSubmission() });
        assertEquals(0, res);

        File[] avroFiles = getAvroFilesForDir(outputDir);
        assertEquals(4, avroFiles.length);
        Schema schema = getSchema(avroFiles[0].getAbsolutePath());
        assertNotNull(schema);
        for (int i = 1; i < avroFiles.length; i++) {
            assertEquals(schema.toString(), getSchema(avroFiles[i].getAbsolutePath()).toString());
        }

        System.out.println("**************************");
        System.out.println("Files = all inputs");
        System.out.println("Number of rows = " + numRows);
        for (Map.Entry<Object, Integer> entry : histogram.entrySet()) {
            System.out.println("Key = " + entry.getKey() + " Pct = " + getPercentage(entry.getValue(), numRows));
        }
        System.out.println("**************************");
        for (File avroFile : avroFiles) {
            histogram = new HashMap<Object, Integer>();
            Pair<Map<Object, Integer>, Integer> retval = computeDistribution(avroFile.getAbsolutePath(), "Event_Latitude_Customer", histogram);
            System.out.println("**************************");
            System.out.println("File = " + avroFile.getName());
            System.out.println("Number of rows = " + retval.value());
            for (Map.Entry<Object, Integer> entry : retval.key().entrySet()) {
                System.out.println("Key = " + entry.getKey() + " Pct = " + getPercentage(entry.getValue(), retval.value()));
            }
            System.out.println("**************************");
        }
    }

    private String getPercentage(int numItems, int totalItems) {
        DecimalFormat df = new DecimalFormat("#.##");
        return df.format((double) numItems / (double) totalItems);
    }

    private Schema getSchema(String filename) throws Exception {
        return getAvroReader(filename).getSchema();
    }

    private FileReader<GenericRecord> getAvroReader(String filename) throws Exception {
        Path path = new Path(filename);
        SeekableInput input = new FsInput(path, new Configuration());
        GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<GenericRecord>();
        return DataFileReader.openReader(input, fileReader);
    }

    private Pair<Map<Object, Integer>, Integer> computeDistribution(String filename, String column, Map<Object, Integer> histogram) throws Exception {
        if (histogram == null) {
            histogram = new HashMap<Object, Integer>();
        }

        FileReader<GenericRecord> reader = getAvroReader(filename);
        int numRows = 0;
        for (Iterator<GenericRecord> it = reader.iterator(); it.hasNext(); ) {
            GenericRecord record = it.next();
            Object value = record.get(column);
            if (histogram.containsKey(value)) {
                histogram.put(value, histogram.get(value)+1);
            } else {
                histogram.put(value, 1);
            }
            numRows++;
        }
        return new Pair<Map<Object, Integer>, Integer>(histogram, numRows);
    }
}
