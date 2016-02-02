package com.latticeengines.dataplatform.runtime.mapreduce.python;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.net.URL;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.MRPathFilter;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.runtime.python.PythonMRJobType;
import com.latticeengines.dataplatform.runtime.python.PythonMRProperty;
import com.latticeengines.domain.exposed.modeling.Classifier;

public class PythonMRJobUnitTestNG {

    private Job job;
    private Properties property;
    private Classifier classifier;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        URL inputUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/runtime/mapreduce/DELL_EVENT_TABLE");
        String inputDir = inputUrl.getPath();

        classifier = PythonMRTestUtils.setupDummyClassifier();
        String cacheFilePath = PythonMRUtils.setupProfilingCacheFiles(classifier, "/app/dataplatform/lib/le-dataplatform-shaded.jar", "1.0.1");
        String cacheArchivePath = PythonMRUtils.setupArchiveFilePath(classifier, "1.0.1");
        String[] tokens = classifier.getPythonPipelineLibHdfsPath().split("/");

        property = new Properties();
        property.put(MapReduceProperty.INPUT.name(), inputDir);
        property.put(MapReduceProperty.OUTPUT.name(), inputDir + "/profile");
        property.put(MapReduceProperty.CUSTOMER.name(), "DELL");
        property.put(MapReduceProperty.QUEUE.name(), "Priority0.MapReduce.0");
        property.put(MapReduceProperty.JOB_TYPE.name(), "profiling");
        property.put(MapReduceProperty.CACHE_FILE_PATH.name(), cacheFilePath);
        property.put(MapReduceProperty.CACHE_ARCHIVE_PATH.name(), cacheArchivePath);
        property.put(PythonMRProperty.LINES_PER_MAP.name(), "1");
        property.put(PythonMRProperty.PYTHONPATH.name(), ".:leframework.tar.gz:" + tokens[tokens.length - 1]);
        property.put(PythonMRProperty.PYTHONIOENCODING.name(), "UTF-8");
        property.put(PythonMRProperty.SHDP_HD_FSWEB.name(), "${dataplatform.fs.web.defaultFS}");
        property.put(PythonContainerProperty.METADATA_CONTENTS.name(), classifier.toString());

        job = Job.getInstance();

    }

    @Test(groups = "unit")
    public void testMRJob() throws Exception {
        PythonMRJob customizer = new PythonMRJob(new Configuration());
        customizer.setVersionManager(new VersionManager(""));
        customizer.customize(job, property);

        Configuration conf = job.getConfiguration();
        String metadata = conf.get(PythonContainerProperty.METADATA_CONTENTS.name());
        assertNotNull(conf.get("mapreduce.job.queuename"));
        assertEquals(metadata, property.get(PythonContainerProperty.METADATA_CONTENTS.name()));
        assertNotNull(conf.get(MapReduceProperty.INPUT.name()));
        assertEquals(conf.get(MRPathFilter.INPUT_FILE_PATTERN), PythonMRJobType.CONFIG_FILE);

        assertEquals(job.getCacheFiles().length, 11);
        assertEquals(job.getCacheArchives().length, 2);
        assertEquals(job.getInputFormatClass(), NLineInputFormat.class);

        assertEquals(classifier.toString(), metadata);

    }
}
