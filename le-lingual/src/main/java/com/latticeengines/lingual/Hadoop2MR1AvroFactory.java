package com.latticeengines.lingual;

import cascading.avro.AvroScheme;
import cascading.lingual.platform.hadoop2.Hadoop2MR1DefaultFactory;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;

public class Hadoop2MR1AvroFactory extends Hadoop2MR1DefaultFactory {
    // Called reflectively
    public String getDescription() {
        return "Avro Format Provider";
    }

    // Called reflectively
    public Scheme createScheme(Fields fields) {
        return new AvroScheme(fields, fields.getTypesClasses());
    }
}
