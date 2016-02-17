package com.latticeengines.domain.exposed.workflow;

public enum SourceFileState {
    Uploaded, // Uploaded into hdfs but not resident as a registered avro file
    Imported // Registered as an avro file for consumption
}
