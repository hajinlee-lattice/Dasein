package com.dataartisans.flink.cascading.types.tuple;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class DefinedTupleSerializerSnapshot extends SimpleTypeSerializerSnapshot<Tuple> {

    public DefinedTupleSerializerSnapshot(Fields fields, TypeSerializer[] fieldSers) {
        super(() -> new DefinedTupleSerializer(fields, fieldSers));
    }

}
