package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class CSVImportLineInputFormat extends FileInputFormat<LongWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(CSVImportLineInputFormat.class);

    private long gigaByte = 1073741824l;

    public CSVImportLineInputFormat() {
    }

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new LineRecordReader();
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList();
        Iterator i$ = this.listStatus(job).iterator();
        while (i$.hasNext()) {
            FileStatus status = (FileStatus) i$.next();
            splits.addAll(getSplitsForFile(job, status));
        }
        return splits;
    }

    private List<FileSplit> getSplitsForFile(JobContext job, FileStatus status) throws IOException {
        Path fileName = status.getPath();
        if (status.isDirectory()) {
            throw new IOException("Not a file: " + fileName);
        } else {
            long totalLength = status.getLen();
            int cores = job.getConfiguration().getInt("mapreduce.map.cpu.vcores", 1);
            if (totalLength > gigaByte) {
                cores = job.getConfiguration().getInt(CSVFileImportProperty.CSV_FILE_MAPPER_CORES.name(), 1);
                job.getConfiguration().setInt("mapreduce.map.cpu.vcores", cores);
            }
            logger.info("Total imported file size is {}， set CPU cores to {}", totalLength, cores);
            return getSplits(job, fileName, totalLength);
        }
    }

    private List<FileSplit> getSplits(JobContext job, Path fileName, long totalLength) {
        List<FileSplit> splits = new ArrayList();
        splits.add(createFileSplit(fileName, 0, totalLength));
        return splits;
    }

    private FileSplit createFileSplit(Path fileName, long begin, long length) {
        return new FileSplit(fileName, begin, length, new String[0]);
    }

}
