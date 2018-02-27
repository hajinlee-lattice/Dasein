package com.latticeengines.domain.exposed.metadata.mds;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;

public interface DecoratorFactory2<T1 extends Serializable, T2 extends Serializable>
        extends DecoratorFactory<Namespace2<T1, T2>> {

    default Decorator getDecorator(T1 coord1, T2 coord2) {
        return getDecorator(Namespace.as(coord1, coord2));
    }

}
