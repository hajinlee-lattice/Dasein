package com.latticeengines.datacloud.core.source;

/*
 * enum to describe what kind of transformation is needed to
 * convert data from original format to expected format
 */
public enum RawTransformationType {
    // transform from csv.tar.gz file to avro
    CSV_TARGZ_TO_AVRO, CSV_TO_AVRO;
}
