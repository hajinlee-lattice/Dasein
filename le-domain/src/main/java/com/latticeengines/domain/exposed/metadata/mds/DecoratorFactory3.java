package com.latticeengines.domain.exposed.metadata.mds;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace3;

public interface DecoratorFactory3<T1 extends Serializable, T2 extends Serializable, T3 extends Serializable>
        extends DecoratorFactory<Namespace3<T1, T2, T3>> {

    default Decorator getDecorator(T1 coord1, T2 coord2, T3 coord3) {
        return getDecorator(Namespace.as(coord1, coord2, coord3));
    }

}
