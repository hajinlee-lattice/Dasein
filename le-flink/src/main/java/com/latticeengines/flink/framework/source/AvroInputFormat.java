package com.latticeengines.flink.framework.source;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroInputFormat extends FileInputFormat<GenericRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AvroInputFormat.class);
    private transient FileReader<GenericRecord> dataFileReader;
    private transient long end;

    public AvroInputFormat(Path filePath) {
        super(filePath);
    }

    public void setUnsplittable(boolean unsplittable) {
        this.unsplittable = unsplittable;
    }

    public TypeInformation<GenericRecord> getProducedType() {
        return TypeInformation.of(GenericRecord.class);
    }

    @SuppressWarnings("unchecked")
    public void open(FileInputSplit split) throws IOException {
        super.open(split);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        LOG.info("Opening split " + split);
        FSDataInputStreamWrapper in = new FSDataInputStreamWrapper(this.stream,
                split.getPath().getFileSystem().getFileStatus(split.getPath()).getLen());
        this.dataFileReader = DataFileReader.openReader(in, (DatumReader) datumReader);
        this.dataFileReader.sync(split.getStart());
        this.end = split.getStart() + split.getLength();
    }

    public boolean reachedEnd() throws IOException {
        return !this.dataFileReader.hasNext() || this.dataFileReader.pastSync(this.end);
    }

    public GenericRecord nextRecord(GenericRecord reuseValue) throws IOException {
        if (this.reachedEnd()) {
            return null;
        } else {
            return this.dataFileReader.next(reuseValue);
        }
    }
}
