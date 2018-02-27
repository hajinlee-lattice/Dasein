package com.latticeengines.domain.exposed.metadata.mds;

import java.io.Serializable;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;

public interface DecoratorFactory1<T extends Serializable> extends DecoratorFactory<Namespace1<T>> {

    default Decorator getDecorator(T coord) {
        return getDecorator(Namespace.as(coord));
    }

}
