package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportProperty;

public class CSVImportReducer extends Reducer<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(CSVImportReducer.class);

    private final String ERROR_FILE = "error.csv";

    private final String ERROR_FILE_NAME = "error";

    private Path outputPath;

    private Configuration yarnConfiguration;

    /**
     * RFC 4180 defines line breaks as CRLF
     */
    private final String CRLF = "\r\n";


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        yarnConfiguration = context.getConfiguration();
        outputPath = MapFileOutputFormat.getOutputPath(context);
        log.info("Path is:" + outputPath);
    }

    @Override
    public void run(Reducer<NullWritable, NullWritable, NullWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        setup(context);
        process();
    }

    private void process() throws IOException {
        try (FileSystem fs = HdfsUtils.getFileSystem(yarnConfiguration, outputPath + "/" + ERROR_FILE)) {
            Path path = new Path(outputPath + "/" + ERROR_FILE);
            if (fs.exists(path)) {
                fs.delete(path, false);
            }
            HdfsUtils.HdfsFilenameFilter hdfsFilenameFilter = filename -> filename.startsWith(ERROR_FILE_NAME);
            List<String> errorPaths = HdfsUtils.getFilesForDir(yarnConfiguration, outputPath.toString(), hdfsFilenameFilter);
            if (errorPaths.size() == 1) {
                // only rename the file if the file length is 1
                fs.rename(new Path(errorPaths.get(0)), new Path(outputPath + "/" + ERROR_FILE));
            } else {
                errorPaths.sort(String::compareTo);
                try (FSDataOutputStream fsDataOutputStream = fs.create(
                        new Path(outputPath + "/" + ERROR_FILE))) {
                    fsDataOutputStream.writeBytes(StringUtils.join(ImportProperty.ERROR_HEADER, ","));
                    fsDataOutputStream.writeBytes(CRLF);
                    for (String errorPath : errorPaths) {
                        try (InputStream inputStream = HdfsUtils.getInputStream(yarnConfiguration, errorPath)) {
                            IOUtils.copy(inputStream, fsDataOutputStream);
                        }
                        fs.delete(new Path(errorPath), false);
                    }
                }
            }
        }
    }

}
