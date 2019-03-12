package com.latticeengines.yarn.exposed.runtime.mapreduce;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.yarn.exposed.client.mapreduce.MRJobCustomization;

public abstract class MRJobCustomizationBase extends Configured implements Tool, MRJobCustomization {

    private static final Logger log = LoggerFactory.getLogger(MRJobCustomizationBase.class);

    public MRJobCustomizationBase(Configuration config) {
        setConf(config);
    }

    public abstract String getJobType();

    public abstract void customize(Job mrJob, Properties properties);

    public abstract int run(String[] args) throws Exception;

    protected static class IgnoreDirectoriesAndSupportOnlyAvroFilesFilter extends Configured implements PathFilter {
        private FileSystem fs;

        public IgnoreDirectoriesAndSupportOnlyAvroFilesFilter() {
            super();
        }

        public IgnoreDirectoriesAndSupportOnlyAvroFilesFilter(Configuration config) {
            super(config);
        }

        @Override
        public boolean accept(Path path) {
            try {
                if (this.getConf().get(FileInputFormat.INPUT_DIR).contains(path.toString())) {
                    // FIXME: temp log for M27
                    log.info("Accept path " + path.toString());
                    return true;
                }
                if (!fs.isDirectory(path) && path.toString().endsWith(".avro")) {
                    // FIXME: temp log for M27
                    log.info("Accept path " + path.toString());
                    return true;
                }
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_00002, e);
            }
            return false;
        }

        @Override
        public void setConf(Configuration config) {
            try {
                if (config != null) {
                    fs = FileSystem.get(config);
                    super.setConf(config);
                }

            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_00002, e);
            }
        }
    }
}
