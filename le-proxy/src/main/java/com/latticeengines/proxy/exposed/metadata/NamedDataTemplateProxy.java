package com.latticeengines.proxy.exposed.metadata;

import java.io.Serializable;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnitStore;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;

import reactor.core.publisher.Flux;

public interface NamedDataTemplateProxy {

    long countSchema(String dtName, String... namespace);

    List<DataUnit> getData(String dtName, String... namespace);

    Flux<ColumnMetadata> getSchema(String dtName, String... namespace);

    Flux<ColumnMetadata> getUnorderedSchema(String dtName, String... namespace);

    <T extends Serializable> DataUnitStore<Namespace1<T>> toDataTemplate(String dtName, Class<T> clz);

    <T1 extends Serializable, T2 extends Serializable> DataUnitStore<Namespace2<T1, T2>> toDataTemplate(String dtName, Class<T1> clz1, Class<T2> clz2);

}
