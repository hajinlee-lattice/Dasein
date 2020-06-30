package com.latticeengines.domain.exposed.spark.common;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableList;

public final class ChangeListConstants {

    protected ChangeListConstants() {
        throw new UnsupportedOperationException();
    }

    public static final String RowId = "RowId";
    public static final String ColumnId = "ColumnId";
    public static final String DataType = "DataType";
    public static final String Deleted = "Deleted";

    public static final String From = "From";
    public static final String To = "To";

    public static final String FromString = From + "String";
    public static final String ToString = To + "String";
    public static final String FromBoolean = From + "Boolean";
    public static final String ToBoolean= To + "Boolean";
    public static final String FromInteger = From + "Integer";
    public static final String ToInteger = To + "Integer";
    public static final String FromLong = From + "Long";
    public static final String ToLong= To + "Long";
    public static final String FromFloat = From + "Float";
    public static final String ToFloat = To + "Float";
    public static final String FromDouble = From + "Double";
    public static final String ToDouble = To + "Double";

    public static final ImmutableList<ImmutablePair<String, Class<?>>> immutableSchema = //
            ImmutableList.<ImmutablePair<String, Class<?>>>builder() //
                    .add(ImmutablePair.of(RowId, String.class)) //
                    .add(ImmutablePair.of(ColumnId, String.class)) //
                    .add(ImmutablePair.of(DataType, String.class)) //
                    .add(ImmutablePair.of(Deleted, Boolean.class)) //
                    .add(ImmutablePair.of(FromString, String.class)) //
                    .add(ImmutablePair.of(ToString, String.class)) //
                    .add(ImmutablePair.of(FromBoolean, Boolean.class)) //
                    .add(ImmutablePair.of(ToBoolean, Boolean.class)) //
                    .add(ImmutablePair.of(FromInteger, Integer.class)) //
                    .add(ImmutablePair.of(ToInteger, Integer.class)) //
                    .add(ImmutablePair.of(FromLong, Long.class)) //
                    .add(ImmutablePair.of(ToLong, Long.class)) //
                    .add(ImmutablePair.of(FromFloat, Float.class)) //
                    .add(ImmutablePair.of(ToFloat, Float.class)) //
                    .add(ImmutablePair.of(FromDouble, Double.class)) //
                    .add(ImmutablePair.of(ToDouble, Double.class)) //
                    .build();

    public static List<Pair<String, Class<?>>> schema() {
        return immutableSchema.stream().map(p -> Pair.<String, Class<?>>of(p.getLeft(), p.getRight())) //
                .collect(Collectors.toList());
    }

}
