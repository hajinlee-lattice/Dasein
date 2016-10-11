package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFormat;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingProperty;
import com.latticeengines.domain.exposed.modeling.SamplingType;

@Test(singleThreaded = true)
public class SamplingTestNG extends DataPlatformFunctionalTestNGBase {
    private static final String TARGET_COLUMN_NAME = "Event_Latitude_Customer";
    private static final Double DEFAULT_ERROR_RANGE = 0.05;
    private static final Double STRIGENT_ERROR_RANGE = 0.01;

    @Autowired
    private ModelingService modelingService;

    @Value("${dataplatform.sampling.parallel.trainingset.number}")
    private String numberOfSamplingTrainingSet;

    private FileSystem fs;

    private int trainingSet;

    private String customer = "DELL-" + suffix;
    private String table = "DELL_EVENT_TABLE_TEST";
    private String localDir = "com/latticeengines/dataplatform/exposed/service/impl/DELL_EVENT_TABLE_TEST";

    private String dataDir;
    private String baseDir;
    private String sampleDir;

    private SampleStat inputStat;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        setupDirPath();
        setupFileSystem();
        setupInputData();
        trainingSet = Integer.parseInt(numberOfSamplingTrainingSet);
    }

    private void setupDirPath() {
        baseDir = customerBaseDir + "/" + customer;
        dataDir = baseDir + "/data/" + table;
        sampleDir = dataDir + "/samples";
    }

    private void setupFileSystem() throws Exception {
        fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(baseDir), true);
        fs.mkdirs(new Path(dataDir));
    }

    private void setupInputData() throws Exception {
        String inputDir = ClassLoader.getSystemResource(localDir).getPath();
        File[] avroFiles = getAvroFilesForDir(inputDir);
        copyInputFilesToHdfs(avroFiles);
        calculateInputStat(avroFiles);
    }

    private void copyInputFilesToHdfs(File[] avroFiles) throws Exception {
        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
        for (File avroFile : avroFiles) {
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(), dataDir, false));
        }
        doCopy(fs, copyEntries);
    }

    private void calculateInputStat(File[] avroFiles) throws Exception {
        Map<String, Double> classLabelToPercentage = new HashMap<String, Double>();
        int totalRows = 0;
        for (File avroFile : avroFiles) {
            int fileRows = calculateClassRowCount("file:" + avroFile.getAbsolutePath(), TARGET_COLUMN_NAME,
                    classLabelToPercentage);
            totalRows += fileRows;
        }
        calculateClassPercentage(classLabelToPercentage, totalRows);
        inputStat = new SampleStat("all inputs", totalRows, classLabelToPercentage);
    }

    private SamplingConfiguration getSamplingConfig() {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setCustomer(customer);
        samplingConfig.setTable(table);
        samplingConfig.setParallelEnabled(true);
        return samplingConfig;
    }

    @Test(groups = { "functional" })
    public void testDefaultSampling() throws Exception {
        SamplingConfiguration samplingConfig = getSamplingConfig();
        checkFinalApplicationStatusSucceeded(samplingConfig);
        List<String> samplingFiles = HdfsUtils.getFilesForDir(yarnConfiguration, sampleDir, HdfsFileFormat.AVRO_FILE);
        assertEquals(samplingFiles.size(), trainingSet + 2);
        List<SampleStat> sampleStats = getSampleStats(samplingFiles);
        checkSampleClassDistribution(sampleStats, DEFAULT_ERROR_RANGE);
        printSamplingStats(samplingConfig.getSamplingType(), sampleStats);
        deleteSamples();
    }

    @Test(groups = { "functional" })
    public void testBootstrapSampling() throws Exception {
        SamplingConfiguration samplingConfig = getSamplingConfig();
        samplingConfig.setSamplingType(SamplingType.BOOTSTRAP_SAMPLING);
        samplingConfig.setProperty(SamplingProperty.TRAINING_DATA_SIZE.name(), "8000");
        samplingConfig.setProperty(SamplingProperty.TRAINING_SET_SIZE.name(), "1600");

        checkFinalApplicationStatusSucceeded(samplingConfig);
        List<String> samplingFiles = HdfsUtils.getFilesForDir(yarnConfiguration, sampleDir, HdfsFileFormat.AVRO_FILE);
        assertEquals(samplingFiles.size(), trainingSet + 2);
        List<SampleStat> sampleStats = getSampleStats(samplingFiles);
        checkSampleClassDistribution(sampleStats, DEFAULT_ERROR_RANGE);
        printSamplingStats(samplingConfig.getSamplingType(), sampleStats);
        deleteSamples();
    }

    @Test(groups = { "functional" })
    public void testStratifiedSampling() throws Exception {
        SamplingConfiguration samplingConfig = getSamplingConfig();
        samplingConfig.setSamplingType(SamplingType.STRATIFIED_SAMPLING);
        samplingConfig.setProperty(SamplingProperty.CLASS_DISTRIBUTION.name(), "0=5496,1=4504");
        samplingConfig.setProperty(SamplingProperty.TARGET_COLUMN_NAME.name(), TARGET_COLUMN_NAME);

        checkFinalApplicationStatusSucceeded(samplingConfig);
        List<String> samplingFiles = HdfsUtils.getFilesForDir(yarnConfiguration, sampleDir, HdfsFileFormat.AVRO_FILE);
        assertEquals(samplingFiles.size(), trainingSet + 2);
        List<SampleStat> sampleStats = getSampleStats(samplingFiles);
        checkSampleClassDistribution(sampleStats, STRIGENT_ERROR_RANGE);
        printSamplingStats(samplingConfig.getSamplingType(), sampleStats);
        deleteSamples();
    }

    @Test(groups = { "functional" })
    public void testUpSampling() throws Exception {
        SamplingConfiguration samplingConfig = getSamplingConfig();
        samplingConfig.setSamplingType(SamplingType.UP_SAMPLING);
        samplingConfig.setProperty(SamplingProperty.TARGET_COLUMN_NAME.name(), TARGET_COLUMN_NAME);
        samplingConfig.setProperty(SamplingProperty.MINORITY_CLASS_LABEL.name(), "1");
        samplingConfig.setProperty(SamplingProperty.MINORITY_CLASS_SIZE.name(), "4504");
        samplingConfig.setProperty(SamplingProperty.UP_TO_PERCENTAGE.name(), "122");

        checkFinalApplicationStatusSucceeded(samplingConfig);
        List<String> samplingFiles = HdfsUtils.getFilesForDir(yarnConfiguration, sampleDir, HdfsFileFormat.AVRO_FILE);
        assertEquals(samplingFiles.size(), trainingSet + 2);
        List<SampleStat> sampleStats = getSampleStats(samplingFiles);
        checkEvenClassDistribution(sampleStats, DEFAULT_ERROR_RANGE);
        printSamplingStats(samplingConfig.getSamplingType(), sampleStats);
        deleteSamples();
    }

    @Test(groups = { "functional" })
    public void testDownSampling() throws Exception {
        SamplingConfiguration samplingConfig = getSamplingConfig();
        samplingConfig.setSamplingType(SamplingType.DOWN_SAMPLING);
        samplingConfig.setProperty(SamplingProperty.TARGET_COLUMN_NAME.name(), TARGET_COLUMN_NAME);
        samplingConfig.setProperty(SamplingProperty.MAJORITY_CLASS_LABEL.name(), "0");
        samplingConfig.setProperty(SamplingProperty.DOWN_TO_PERCENTAGE.name(), "82");

        checkFinalApplicationStatusSucceeded(samplingConfig);
        List<String> samplingFiles = HdfsUtils.getFilesForDir(yarnConfiguration, sampleDir, HdfsFileFormat.AVRO_FILE);
        assertEquals(samplingFiles.size(), trainingSet + 2);
        List<SampleStat> sampleStats = getSampleStats(samplingFiles);
        checkEvenClassDistribution(sampleStats, DEFAULT_ERROR_RANGE);
        printSamplingStats(samplingConfig.getSamplingType(), sampleStats);
        deleteSamples();
    }

    private void checkFinalApplicationStatusSucceeded(SamplingConfiguration samplingConfig) throws Exception {
        ApplicationId appId = modelingService.createSamples(samplingConfig);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    private void checkSampleClassDistribution(List<SampleStat> sampleStats, Double errorRange) {
        for (SampleStat stat : sampleStats) {
            checkClassDistribution(stat, inputStat.classLabelToPercentage, errorRange);
        }
    }

    private void checkEvenClassDistribution(List<SampleStat> sampleStats, Double errorRange) {
        for (SampleStat stat : sampleStats) {
            if (stat.fileName.contains("Test")) {
                checkClassDistribution(stat, inputStat.classLabelToPercentage, errorRange);
            } else {
                checkClassDistribution(stat, getEvenDistribution(), errorRange);
            }
        }
    }

    private Map<String, Double> getEvenDistribution() {
        Map<String, Double> evenDistribution = new HashMap<String, Double>();
        evenDistribution.put("0", 0.5);
        evenDistribution.put("1", 0.5);
        return evenDistribution;
    }

    private void checkClassDistribution(SampleStat stat, Map<String, Double> expectedDistribution, Double errorRange) {
        Map<String, Double> sampleClassLabelToPercentage = stat.classLabelToPercentage;
        for (String classLabel : sampleClassLabelToPercentage.keySet()) {
            Double expectedPercentage = expectedDistribution.get(classLabel);
            Double samplePercentage = sampleClassLabelToPercentage.get(classLabel);
            Double error = Math.abs(samplePercentage - expectedPercentage);
            assertTrue(error.compareTo(errorRange) <= 0);
        }
    }

    private List<SampleStat> getSampleStats(List<String> samplingFiles) throws Exception {
        List<SampleStat> sampleStats = new ArrayList<SampleStat>();
        for (String file : samplingFiles) {
            Path path = new Path(file);
            Map<String, Double> classLabelToPercentage = new HashMap<String, Double>();
            int totalRows = calculateClassRowCount(file, TARGET_COLUMN_NAME, classLabelToPercentage);
            calculateClassPercentage(classLabelToPercentage, totalRows);
            SampleStat stat = new SampleStat(path.getName(), totalRows, classLabelToPercentage);
            sampleStats.add(stat);
        }
        return sampleStats;
    }

    private void calculateClassPercentage(Map<String, Double> classLabelToPercentage, Integer totalRows)
            throws Exception {
        for (String classLabel : classLabelToPercentage.keySet()) {
            Double percentage = classLabelToPercentage.get(classLabel) / totalRows;
            classLabelToPercentage.put(classLabel, percentage);
        }
    }

    private int calculateClassRowCount(String filename, String column, Map<String, Double> classLabelToPercentage)
            throws Exception {

        FileReader<GenericRecord> reader = getAvroReader(filename);
        int numRows = 0;
        for (Iterator<GenericRecord> it = reader.iterator(); it.hasNext();) {
            GenericRecord record = it.next();
            String classLabel = record.get(column).toString();
            if (classLabelToPercentage.containsKey(classLabel)) {
                classLabelToPercentage.put(classLabel, classLabelToPercentage.get(classLabel) + 1);
            } else {
                classLabelToPercentage.put(classLabel, 1.0);
            }
            numRows++;
        }
        return numRows;
    }

    private FileReader<GenericRecord> getAvroReader(String filename) throws Exception {
        Path path = new Path(filename);
        SeekableInput input = new FsInput(path, new Configuration());
        GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<GenericRecord>();
        return DataFileReader.openReader(input, fileReader);
    }

    private void printSamplingStats(SamplingType samplingType, List<SampleStat> sampleStats) throws Exception {
        System.out.println("**************************");
        System.out.println(samplingType.name());
        System.out.println("**************************");
        printSampleStat(inputStat);
        for (SampleStat stat : sampleStats) {
            printSampleStat(stat);
        }
    }

    private void printSampleStat(SampleStat stat) {
        System.out.println("**************************");
        System.out.println("Files = " + stat.fileName);
        System.out.println("Number of rows = " + stat.rowCount);
        for (Map.Entry<String, Double> entry : stat.classLabelToPercentage.entrySet()) {
            System.out.println("Key = " + entry.getKey() + " Pct = " + getPercentageString(entry.getValue()));
        }
        System.out.println("**************************");
    }

    private String getPercentageString(Double value) {
        DecimalFormat df = new DecimalFormat("#.##");
        return df.format(value);
    }

    private void deleteSamples() throws IllegalArgumentException, IOException {
        fs.delete(new Path(sampleDir), true);
    }

    @AfterClass(groups = { "functional" })
    public void cleanup() throws Exception {
        fs.delete(new Path(baseDir), true);
    }

    private class SampleStat {
        protected String fileName;
        protected int rowCount;
        protected Map<String, Double> classLabelToPercentage;

        protected SampleStat(String fileName, int rowCount, Map<String, Double> classLabelToPercentage) {
            this.fileName = fileName;
            this.rowCount = rowCount;
            this.classLabelToPercentage = classLabelToPercentage;
        }
    }
}
