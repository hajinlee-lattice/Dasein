package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

public class ParquetUtils {

    private static final Logger log = LoggerFactory.getLogger(ParquetUtils.class);

    public static Schema getAvroSchema(Configuration configuration, String globPath) {
        List<String> matchedFiles = HdfsUtils.getAllMatchedFiles(configuration, globPath);
        if (CollectionUtils.isEmpty(matchedFiles)) {
            throw new IllegalArgumentException("Cannot find any file matching glob " + globPath);
        }
        String filePath = matchedFiles.get(0);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            try {
                InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), configuration);
                try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                    AvroSchemaConverter converter = new AvroSchemaConverter(configuration);
                    MessageType parquetSchema = reader.getFileMetaData().getSchema();
                    return converter.convert(parquetSchema);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse schema for parquet file " + filePath);
            }
        });
    }

    public static long countParquetFiles(Configuration configuration, String... globPaths) {
        List<String> matchedFiles = HdfsUtils.getAllMatchedFiles(configuration, globPaths);
        if (CollectionUtils.isEmpty(matchedFiles)) {
            return 0L;
        } else {
            AtomicLong count = new AtomicLong(0L);
            matchedFiles.forEach(filePath -> count.addAndGet(countOneFile(configuration, filePath)));
            return count.get();
        }
    }

    private static long countOneFile(Configuration configuration, String filePath) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> {
            try {
                InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), configuration);
                try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
                    return reader.getRecordCount();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static ParquetFilesIterator iteratorParquetFiles(Configuration configuration, String... globPaths) {
        try {
            return new ParquetFilesIterator(configuration, globPaths);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ParquetFilesIterator implements AvroRecordIterator {

        private List<String> matchedFiles;
        private Integer fileIdx = 0;
        private ParquetReader<GenericRecord> reader;
        private GenericRecord nextRecord = null;
        private Configuration configuration;

        ParquetFilesIterator(Configuration configuration, String... globPaths) throws IOException {
            matchedFiles = HdfsUtils.getAllMatchedFiles(configuration, globPaths);
            if (CollectionUtils.isEmpty(matchedFiles)) {
                log.warn("Could not find any parquet file that matches one of the path patterns [ "
                        + StringUtils.join(globPaths, ", ") + " ]");
            } else {
                this.configuration = configuration;
                loadNextFile();
            }
        }

        private ParquetReader<GenericRecord> getParquetFileReader(String filePath) throws IOException {
            InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), configuration);
            log.debug("Opening a parquet reader at " + filePath);
            return AvroParquetReader.<GenericRecord>builder(inputFile).build();
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        @Override
        public GenericRecord next() {
            if (nextRecord == null) {
                throw new NoSuchElementException();
            }
            GenericRecord toReturn = nextRecord;
            try {
                nextRecord = reader.read();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read next record from parquet file.", e);
            }
            if (nextRecord == null) {
                // need to switch to next file
                try {
                    loadNextFile();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to load next parquet file.", e);
                }
            }
            return toReturn;
        }

        private void loadNextFile() throws IOException {
            while (nextRecord == null && fileIdx < matchedFiles.size()) {
                closeReader();
                reader = getParquetFileReader(matchedFiles.get(fileIdx++));
                nextRecord = reader.read();
            }
        }

        private void closeReader() {
            if (reader == null) {
                return;
            }
            try {
                log.debug("Closing parquet reader.");
                reader.close();
            } catch (IOException e) {
                log.error("Failed to close parquet file reader.");
            } finally {
                reader = null;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not applicable to this iterator.");
        }

        @Override
        public void close() {
            closeReader();
        }

    }

}
