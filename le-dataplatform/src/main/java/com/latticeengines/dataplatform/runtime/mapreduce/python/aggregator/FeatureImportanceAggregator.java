package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;

public class FeatureImportanceAggregator implements FileAggregator {
    private static final Log log = LogFactory.getLog(FeatureImportanceAggregator.class);

    @SuppressWarnings("rawtypes")
    @Override
    public void aggregate(List<String> localPaths, Configuration config, Context context) throws Exception {
        HashMap<String, Double> featureImportanceValues = aggregateImportanceValues(localPaths);
        writeToLocal(featureImportanceValues);
        copyToHdfs(config);
    }

    public HashMap<String, Double> aggregateImportanceValues(List<String> localPaths) throws IOException {
        HashMap<String, Double> featureImportanceValues = new HashMap<String, Double>();
        String line = "";

        for (String path : localPaths) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))) {
                line = reader.readLine();

                while ((line = reader.readLine()) != null) {
                    int commaIndex = line.indexOf(",");
                    String feature = line.substring(0, commaIndex).trim();
                    double importance = Double.parseDouble(line.substring(commaIndex + 1).trim());

                    if (featureImportanceValues.containsKey(feature)) {
                        featureImportanceValues.put(feature, featureImportanceValues.get(feature) + importance);
                    } else {
                        featureImportanceValues.put(feature, importance);
                    }
                }
            }
        }

        int N = localPaths.size();
        for (String feature : featureImportanceValues.keySet()) {
            featureImportanceValues.put(feature, featureImportanceValues.get(feature) / N);
        }

        return featureImportanceValues;
    }

    private void writeToLocal(HashMap<String, Double> FeatureImportanceValues) {
        try (FileWriter fwriter = new FileWriter(getName())) {
            try (BufferedWriter bwriter = new BufferedWriter(fwriter)) {
                bwriter.write("Column Name, Feature Importance\n");
                List<Map.Entry<String, Double>> sortedFeatures = new LinkedList<Map.Entry<String, Double>>(
                        FeatureImportanceValues.entrySet());

                Collections.sort(sortedFeatures, new Comparator<Map.Entry<String, Double>>() {

                    @Override
                    public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                        return (o2.getValue()).compareTo(o1.getValue());
                    }

                });

                for (Map.Entry<String, Double> feature : sortedFeatures) {
                    String fiValue = BigDecimal.valueOf(feature.getValue()).toPlainString();
                    bwriter.write(feature.getKey() + ", " + fiValue.substring(0, Math.min(fiValue.length(), 8)) + "\n");
                }
            } catch (IOException ex) {
                log.warn("There was a problem in writing to local feature importance aggregation file.");
            }
        } catch (IOException ex) {
            log.warn("There was a problem opening feature importance output file.");
        }
    }

    private void copyToHdfs(Configuration config) throws Exception {
        String hdfsPath = config.get(MapReduceProperty.OUTPUT.name());
        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }

                String name = file.getPath().getName().toString();
                return name.equals("diagnostics.json");
            }

        };
        try {
            List<FileStatus> files = HdfsUtils.getFileStatusesForDirRecursive(config, hdfsPath, filter, true);
            hdfsPath = files.get(0).getPath().getParent().toString();
        } catch (IOException e) {
        }

        HdfsUtils.copyLocalToHdfs(config, getName(), hdfsPath);
    }

    @Override
    public String getName() {
        return FileAggregator.FEATURE_IMPORTANCE_TXT;
    }

}
