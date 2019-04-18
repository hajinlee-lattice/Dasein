package com.dataartisans.flink.cascading.types.tuplearray;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import cascading.tuple.Tuple;

public class TupleArraySerializerSnapshot extends CompositeTypeSerializerSnapshot<Tuple[], TupleArraySerializer> {

    private static final int CURRENT_VERSION = 1;
    private final int length;

    public TupleArraySerializerSnapshot(int length) {
        super(TupleArraySerializer.class);
        this.length = length;
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected TupleArraySerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
        return new TupleArraySerializer(length, (TypeSerializer<Tuple>[]) nestedSerializers);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(TupleArraySerializer outerSerializer) {
        return outerSerializer.getTupleSerializers();
    }
}
