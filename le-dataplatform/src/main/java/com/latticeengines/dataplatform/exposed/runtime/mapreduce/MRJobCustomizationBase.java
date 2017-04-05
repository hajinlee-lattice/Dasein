package com.latticeengines.dataplatform.exposed.runtime.mapreduce;

import java.util.Properties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.latticeengines.dataplatform.exposed.client.mapreduce.MRJobCustomization;

public abstract class MRJobCustomizationBase extends Configured implements Tool, MRJobCustomization {

    public abstract String getJobType();

    public abstract void customize(Job mrJob, Properties properties);

    public abstract int run(String[] args) throws Exception;

}
