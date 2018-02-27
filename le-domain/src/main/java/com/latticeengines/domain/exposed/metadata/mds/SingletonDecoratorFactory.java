package com.latticeengines.domain.exposed.metadata.mds;

import com.latticeengines.domain.exposed.metadata.namespace.Namespace0;

public class SingletonDecoratorFactory implements DecoratorFactory<Namespace0> {

    private final Decorator decorator;

    public static SingletonDecoratorFactory from(Decorator decorator) {
        return new SingletonDecoratorFactory(decorator);
    }

    protected SingletonDecoratorFactory(Decorator decorator) {
        this.decorator =  decorator;
    }

    @Override
    public Decorator getDecorator(Namespace0 namespace0) {
        return decorator;
    }

}
