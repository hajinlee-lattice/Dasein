package com.latticeengines.common.exposed.util;

import java.io.Closeable;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;

public interface AvroRecordIterator extends Iterator<GenericRecord>, Closeable {
}
