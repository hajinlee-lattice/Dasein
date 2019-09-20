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

@InterfaceAudience.Public
@InterfaceStability.Stable
public class CSVImportLineInputFormat extends FileInputFormat<LongWritable, Text> {

    private long gigaByte = 1073741824l;

    public CSVImportLineInputFormat() {
    }

    public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new LineRecordReader();
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList();
        int mapperNums = getMapperNums(job);
        Iterator i$ = this.listStatus(job).iterator();
        while (i$.hasNext()) {
            FileStatus status = (FileStatus) i$.next();
            splits.addAll(getSplitsForFile(job, status, mapperNums));
        }
        return splits;
    }

    private List<FileSplit> getSplitsForFile(JobContext job, FileStatus status, int mapperNums) throws IOException {
        Path fileName = status.getPath();
        if (status.isDirectory()) {
            throw new IOException("Not a file: " + fileName);
        } else {
            long totalLength = status.getLen();
            long blockSize = status.getBlockSize();
            if (totalLength <= blockSize) {
                setCSVFileBlockSize(job, -1l);
                List<FileSplit> splits = new ArrayList();
                splits.add(createFileSplit(fileName, 0, totalLength - 1));
                return splits;
            }
            if (totalLength > blockSize && totalLength <= gigaByte) {
                // user 4 mappers while file is greater than block size and less than one giga byte
                int realMapperNums = mapperNums / 2;
                return getSplits(job, fileName, totalLength, realMapperNums);
            }
            return getSplits(job, fileName, totalLength, mapperNums);
        }
    }

    private List<FileSplit> getSplits(JobContext job, Path fileName, long totalLength, int mapperNums) {
        List<FileSplit> splits = new ArrayList();
        long splitSize = totalLength / mapperNums;
        setCSVFileBlockSize(job, splitSize);
        int index = 0;
        while (index < mapperNums) {
            long start = index * splitSize;
            if (index == mapperNums - 1) {
                splits.add(createFileSplit(fileName, start, totalLength - start));
            } else {
                splits.add(createFileSplit(fileName, start, splitSize));
            }
            index++;
        }
        return splits;
    }

    private FileSplit createFileSplit(Path fileName, long begin, long length) {
        return new FileSplit(fileName, begin, length, new String[0]);
    }

    private void setCSVFileBlockSize(JobContext job, long blockSize) {
        job.getConfiguration().setLong(CSVFileImportProperty.CSV_FILE_BLOCK_SIZE.name(), blockSize);
    }

    private int getMapperNums(JobContext job) {
        return job.getConfiguration().getInt(CSVFileImportProperty.CSV_FILE_NUM_MAPPERS.name(), 8);
    }
}
