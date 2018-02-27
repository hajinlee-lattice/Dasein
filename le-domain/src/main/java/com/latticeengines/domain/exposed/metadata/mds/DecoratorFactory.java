package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace;

public interface DecoratorFactory<N extends Namespace> {

    Decorator getDecorator(N namespace);

}
