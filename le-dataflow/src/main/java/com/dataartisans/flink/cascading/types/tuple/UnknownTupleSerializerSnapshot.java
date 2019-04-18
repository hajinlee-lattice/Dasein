package com.dataartisans.flink.cascading.types.tuple;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import cascading.tuple.Tuple;

public class UnknownTupleSerializerSnapshot extends SimpleTypeSerializerSnapshot<Tuple> {

    public UnknownTupleSerializerSnapshot(TypeSerializer fieldSer) {
        super(() -> new UnknownTupleSerializer(fieldSer));
    }

}
