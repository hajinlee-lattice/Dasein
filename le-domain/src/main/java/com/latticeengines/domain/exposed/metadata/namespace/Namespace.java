package com.latticeengines.domain.exposed.metadata.namespace;

import java.io.Serializable;
import java.util.Arrays;

public abstract class Namespace {

    public static <N1 extends Serializable> Namespace1<N1> as(N1 coord1) {
        return new Namespace1<>(coord1);
    }

    public static <N1 extends Serializable, N2 extends Serializable> Namespace2<N1, N2> as(
            N1 coord1, N2 coord2) {
        return new Namespace2<>(coord1, coord2);
    }

    public static <N1 extends Serializable, N2 extends Serializable, N3 extends Serializable> Namespace3<N1, N2, N3> as(
            N1 coord1, N2 coord2, N3 coord3) {
        return new Namespace3<>(coord1, coord2, coord3);
    }

    public static <N1 extends Serializable, N2 extends Serializable, N3 extends Serializable,
            N4 extends Serializable> Namespace4<N1, N2, N3, N4> as(N1 coord1, N2 coord2, N3 coord3, N4 coord4) {
        return new Namespace4<>(coord1, coord2, coord3, coord4);
    }

    public abstract Serializable[] coords();

    @Override
    public String toString() {
        return Arrays.toString(coords());
    }

}
