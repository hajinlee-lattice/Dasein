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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;

public class FeatureImportanceAggregator implements FileAggregator {
    private static final Logger log = LoggerFactory.getLogger(FeatureImportanceAggregator.class);

    @Override
    public void aggregate(List<String> localPaths, Configuration config, Progressable progressable) throws Exception {
        HashMap<String, Double> featureImportanceValues = new HashMap<String, Double>();
        HashMap<String, String> featureDisplayNames = new HashMap<>();
        aggregateImportanceValues(localPaths, featureImportanceValues, featureDisplayNames);
        writeToLocal(featureImportanceValues, featureDisplayNames);
        copyToHdfs(config);
    }

    public void aggregateImportanceValues(List<String> localPaths, HashMap<String, Double> featureImportanceValues,
            HashMap<String, String> featureDisplayNames) throws IOException {
        String line = "";

        for (String path : localPaths) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))) {
                line = reader.readLine();
                while ((line = reader.readLine()) != null) {
                    int firstIndex = StringUtils.ordinalIndexOf(line, ",", 1);
                    int secondIndex = StringUtils.ordinalIndexOf(line, ",", 2);
                    String feature = line.substring(0, firstIndex).trim();
                    double importance = Double.parseDouble(line.substring(firstIndex + 1, secondIndex).trim());
                    String displayName = line.substring(secondIndex + 1).trim();

                    if (featureImportanceValues.containsKey(feature)) {
                        featureImportanceValues.put(feature, featureImportanceValues.get(feature) + importance);
                    } else {
                        featureImportanceValues.put(feature, importance);
                    }
                    featureDisplayNames.put(feature, displayName);
                }
            }
        }

        int N = localPaths.size();
        for (String feature : featureImportanceValues.keySet()) {
            featureImportanceValues.put(feature, featureImportanceValues.get(feature) / N);
        }
    }

    private void writeToLocal(HashMap<String, Double> FeatureImportanceValues,
            HashMap<String, String> featureDisplayNames) {
        try (FileWriter fwriter = new FileWriter(getName())) {
            try (BufferedWriter bwriter = new BufferedWriter(fwriter)) {
                bwriter.write("Column Name,Feature Importance,Column Display Name\n");
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
                    bwriter.write(feature.getKey() + "," + fiValue.substring(0, Math.min(fiValue.length(), 8)) + ","
                            + getDisplayName(featureDisplayNames, feature.getKey()) + "\n");
                }
            } catch (IOException ex) {
                log.warn("There was a problem in writing to local feature importance aggregation file.");
            }
        } catch (IOException ex) {
            log.warn("There was a problem opening feature importance output file.");
        }
    }

    private String getDisplayName(HashMap<String, String> featureDisplayNames, String feature) {
        String displayName = featureDisplayNames.get(feature);
        if (StringUtils.isBlank(displayName)) {
            return feature;
        }
        return displayName;
    }

    private void copyToHdfs(Configuration config) throws Exception {
        String hdfsPath = config.get(MapReduceProperty.OUTPUT.name());
        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }

                String name = file.getPath().getName();
                return name.equals("diagnostics.json");
            }

        };
        try {
            List<FileStatus> files = HdfsUtils.getFileStatusesForDirRecursive(config, hdfsPath, filter, true);
            hdfsPath = files.get(0).getPath().getParent().toString();
        } catch (IOException e) {
            log.warn("Failed to find a hdfs file using the provided path and filter", e);
        }

        HdfsUtils.copyLocalToHdfs(config, getName(), hdfsPath);
    }

    @Override
    public String getName() {
        return FileAggregator.FEATURE_IMPORTANCE_TXT;
    }

}
