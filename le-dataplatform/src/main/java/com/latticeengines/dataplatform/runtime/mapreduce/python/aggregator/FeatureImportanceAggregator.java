package com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;

public class FeatureImportanceAggregator implements FileAggregator {

	@Override
	public void aggregate(List<String> localPaths, Configuration config) throws Exception{
		HashMap<String, Double> FeatureImportanceValues = aggregateImportanceValues(localPaths);
		writeToLocal(FeatureImportanceValues);
		copyToHdfs(config);
	}

	public HashMap<String, Double> aggregateImportanceValues(List<String> localPaths) throws IOException{
		HashMap<String, Double> FeatureImportanceValues = new HashMap<String, Double>();
		String line = "";

		for(String path : localPaths) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            line = reader.readLine();
            
            while ((line = reader.readLine()) != null) {
            	int commaIndex = line.indexOf(",");
            	String feature = line.substring(0, commaIndex).trim();
            	double importance = Double.parseDouble(line.substring(commaIndex+1).trim());
                
            	if(FeatureImportanceValues.containsKey(feature)){
            		FeatureImportanceValues.put(feature, FeatureImportanceValues.get(feature) + importance);
            	}
            	else{
            		FeatureImportanceValues.put(feature, importance);
            	}
            }            
            reader.close();
        }
		
        int N = localPaths.size();
        for (String feature : FeatureImportanceValues.keySet()) {
        	FeatureImportanceValues.put(feature, FeatureImportanceValues.get(feature)/N);
        }
        
        return FeatureImportanceValues;
	}

	private void writeToLocal(HashMap<String, Double> FeatureImportanceValues) throws IOException{
		FileWriter fwriter = new FileWriter(getName());
        BufferedWriter bwriter = new BufferedWriter(fwriter);

        bwriter.write("Column Name, Feature Importance\n");
        for (String feature : FeatureImportanceValues.keySet()){
        	String FIValue = BigDecimal.valueOf(FeatureImportanceValues.get(feature)).toPlainString();
        	bwriter.write(feature + ", " + FIValue.substring(0, Math.min(FIValue.length(), 8)) +"\n");
        }
        bwriter.flush();
        fwriter.flush();
	}
	
    private void copyToHdfs(Configuration config) throws Exception {
        String hdfsPath = config.get(MapReduceProperty.OUTPUT.name());
        HdfsUtils.copyLocalToHdfs(config, getName(), hdfsPath);
    }
    
    @Override
    public String getName() {
        return FileAggregator.FEATURE_IMPORTANCE_TXT;
    }

}
