package com.latticeengines.propdata.core.source;

/*
 * for this source model, original data needs to be transformaed into
 * expected fromat like avro
 */
public interface TransformedToAvroSource extends DataImportedFromHDFS {
    /*
     * enum to describe what kind of transformation is needed to convert data
     * from original format to expected format
     */
    RawTransformationType getTransformationType();
}
