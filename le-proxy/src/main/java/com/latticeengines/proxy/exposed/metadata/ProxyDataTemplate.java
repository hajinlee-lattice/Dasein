package com.latticeengines.proxy.exposed.metadata;

import java.io.Serializable;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnitStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ProxyDataTemplate<N extends Namespace> implements DataUnitStore<N> {

    private final NamedDataTemplateProxy namedDataTemplateProxy;
    private final String dtName;
    private static final Scheduler scheduler = Schedulers.newParallel("proxy-data-template");

    private ProxyDataTemplate(NamedDataTemplateProxy namedDataTemplateProxy, String dtName) {
        this.namedDataTemplateProxy = namedDataTemplateProxy;
        this.dtName = dtName;
    }

    public static <T extends Serializable> ProxyDataTemplate<Namespace1<T>> build(NamedDataTemplateProxy namedDataTemplateProxy,
                                                                                  String dtName, Class<T> clz) {
        return new ProxyDataTemplate<>(namedDataTemplateProxy, dtName);
    }

    public static <T1 extends Serializable, T2 extends Serializable> ProxyDataTemplate<Namespace2<T1, T2>> build(
            NamedDataTemplateProxy namedDataTemplateProxy, String dtName, Class<T1> clz1, Class<T2> clz2) {
        return new ProxyDataTemplate<>(namedDataTemplateProxy, dtName);
    }

    @Override
    public List<DataUnit> getData(N namespace) {
        return namedDataTemplateProxy.getData(dtName, convertNamespace(namespace));
    }

    @Override
    public Flux<ColumnMetadata> getSchema(N namespace) {
        return namedDataTemplateProxy.getSchema(dtName, convertNamespace(namespace));
    }

    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(N namespace) {
        return namedDataTemplateProxy.getUnorderedSchema(dtName, convertNamespace(namespace)) //
                .parallel().runOn(scheduler);
    }

    private String[] convertNamespace(N namespace) {
        Serializable[] tokens = namespace.coords();
        String[] keys = new String[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            keys[i] = tokens[i].toString();
        }
        return keys;
    }

}
