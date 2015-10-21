package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.AggregatorFactory;
import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.DiagnosticsJsonAggregator;
import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.FileAggregator;
import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.ModelPickleAggregator;
import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.ProfileAvroAggregator;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class PythonReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
    private static final Log log = LogFactory.getLog(PythonReducer.class);

    private Configuration config;

    private AggregatorFactory aggregatorFactory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        config = context.getConfiguration();
        aggregatorFactory = AggregatorFactory.getInstance();
        aggregatorFactory.registerFileAggregators(getFileAggregators());
    }

    private Map<String, FileAggregator> getFileAggregators() {
        Map<String, FileAggregator> mappings = new HashMap<String, FileAggregator>();
        ProfileAvroAggregator pAggregator = new ProfileAvroAggregator();
        DiagnosticsJsonAggregator dAggregator = new DiagnosticsJsonAggregator();
        ModelPickleAggregator mAggregator = new ModelPickleAggregator();

        mappings.put(pAggregator.getName(), pAggregator);
        mappings.put(dAggregator.getName(), dAggregator);
        mappings.put(mAggregator.getName(), mAggregator);
        return mappings;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String targetFile = key.toString();
        List<String> inputFiles = localizeFiles(targetFile, values);
        FileAggregator aggregator = aggregatorFactory.getAggregator(targetFile);

        log.info("Aggregating " + key.toString() + " from " + inputFiles.size() + " files");
        try {
            aggregator.aggregate(inputFiles, config);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_12011, e, new String[] { targetFile + " aggregation failed" });
        }
        log.info(targetFile + "Aggregation completed");
    }

    private List<String> localizeFiles(String targetfile, Iterable<Text> values) {
        List<String> inputFiles = new ArrayList<String>();
        try {
            for (Text value : values) {
                String hdfsPath = value.toString() + "/" + targetfile;
                String localPath = "./" + UUID.randomUUID().toString() + "_" + targetfile;
                HdfsUtils.copyHdfsToLocal(config, hdfsPath, localPath);
                inputFiles.add(localPath);
                log.info("Localizing " + hdfsPath + " to " + localPath);
            }

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_12011, e, new String[] { "Failed to localize " + targetfile
                    + " for aggregation" });
        }

        return inputFiles;
    }

}
