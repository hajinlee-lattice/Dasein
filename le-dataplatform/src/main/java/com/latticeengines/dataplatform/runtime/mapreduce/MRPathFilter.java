package com.latticeengines.dataplatform.runtime.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class MRPathFilter extends Configured implements PathFilter {

    public static final String INPUT_FILE_PATTERN = "input.file.pattern";

    private FileSystem fs;
    private Pattern pattern;

    @Override
    public boolean accept(Path path) {
        try {

            if (this.getConf().get(FileInputFormat.INPUT_DIR).contains(path.toString())) {
                return true;
            }
            if (!fs.isDirectory(path)) {
                Matcher m = pattern.matcher(path.toString());
                return m.matches();
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        return false;
    }

    @Override
    public void setConf(Configuration config) {
        super.setConf(config);
        try {
            if (config != null) {
                fs = FileSystem.get(config);
                pattern = Pattern.compile(config.get(INPUT_FILE_PATTERN));
            }

        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }
}
