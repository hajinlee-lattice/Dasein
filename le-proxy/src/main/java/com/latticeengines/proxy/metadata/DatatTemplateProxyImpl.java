package com.latticeengines.proxy.metadata;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace2;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.metadata.DataTemplateProxy;
import com.latticeengines.proxy.exposed.metadata.ProxyDataTemplate;

import reactor.core.publisher.Flux;


@Component("dataTemplateProxy")
public class DatatTemplateProxyImpl extends MicroserviceRestApiProxy implements DataTemplateProxy {

    protected DatatTemplateProxyImpl() {
        super("metadata/datatemplate");
    }

    @Override
    public long countSchema(String dtName, String... namespace) {
        String url = url(dtName, namespace) + "/count";
        return get("count schema", url, Long.class);
    }

    @Override
    public List<DataUnit> getData(String dtName, String... namespace) {
        String url = url(dtName, namespace);
        return getFlux("get data unit", url, DataUnit.class).collectList().block();
    }

    @Override
    public Flux<ColumnMetadata> getSchema(String dtName, String... namespace) {
        String url = url(dtName, namespace);
        return getFlux("get schema", url, ColumnMetadata.class);
    }

    @Override
    public Flux<ColumnMetadata> getUnorderedSchema(String dtName, String... namespace) {
        String url = url(dtName, namespace) + "?unordered=1";
        return getFlux("get unordered schema", url, ColumnMetadata.class);
    }

    @Override
    public <T extends Serializable> DataTemplate<Namespace1<T>> toDataTemplate(String dtName, Class<T> clz) {
        return ProxyDataTemplate.build(this, dtName, clz);
    }

    @Override
    public <T1 extends Serializable, T2 extends Serializable> DataTemplate<Namespace2<T1, T2>> toDataTemplate(String dtName, Class<T1> clz1, Class<T2> clz2) {
        return ProxyDataTemplate.build(this, dtName, clz1, clz2);
    }

    private String url(String dtName, String... namespace) {
        return constructUrl("/{dtName}/namespace/{namespace}", dtName,
                StringUtils.join(namespace, ","));
    }


}
