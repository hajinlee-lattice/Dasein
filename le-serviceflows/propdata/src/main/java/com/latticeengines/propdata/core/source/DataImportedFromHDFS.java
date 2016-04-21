package com.latticeengines.propdata.core.source;

import com.latticeengines.domain.exposed.camille.Path;

/*
 * source model for which data is imported from HDFS staging location
 */
public interface DataImportedFromHDFS extends IngestedRawSource {
    /*
     * HDFS directory for firehose data. stages data will be versioned under
     * this directory
     */
    Path getHDFSPathToImportFrom();

    String getTransformationServiceBeanName();
}
