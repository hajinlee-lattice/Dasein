package com.latticeengines.metadata.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.metadata.mds.NamedDataTemplate;
import com.latticeengines.metadata.mds.impl.TableTemplate;
import com.latticeengines.metadata.service.NamedDataTemplateService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@SuppressWarnings("rawtypes")
@Service("namedDataTemplateService")
public class NamedDataTemplateServiceImpl implements NamedDataTemplateService {

    @Inject
    private TableTemplate tableTemplate;

    private ConcurrentMap<String, NamedDataTemplate> registry;

    @SuppressWarnings("unchecked")
    @Override
    public List<DataUnit> getData(String dataTemplateName, String... namespace) {
        NamedDataTemplate dataTemplate = getDataTemplate(dataTemplateName);
        Namespace keys = dataTemplate.parseNameSpace(namespace);
        return dataTemplate.getData(keys);
    }

    @SuppressWarnings("unchecked")
    @Override
    public long getSchemaCount(String dataTemplateName, String... namespace) {
        NamedDataTemplate dataTemplate = getDataTemplate(dataTemplateName);
        Namespace keys = dataTemplate.parseNameSpace(namespace);
        return dataTemplate.countSchema(keys);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Flux<ColumnMetadata> getSchema(String dataTemplateName, String... namespace) {
        NamedDataTemplate dataTemplate = getDataTemplate(dataTemplateName);
        Namespace keys = dataTemplate.parseNameSpace(namespace);
        return dataTemplate.getSchema(keys);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(String dataTemplateName, String... namespace) {
        NamedDataTemplate dataTemplate = getDataTemplate(dataTemplateName);
        Namespace keys = dataTemplate.parseNameSpace(namespace);
        return dataTemplate.getUnorderedSchema(keys);
    }

    private NamedDataTemplate getDataTemplate(String dataTemplateName) {
        registerDataTemplates();
        if (registry.containsKey(dataTemplateName)) {
            return registry.get(dataTemplateName);
        } else {
            throw new RuntimeException("Cannot find metadata store named " + dataTemplateName);
        }
    }

    private void registerDataTemplates() {
        if (MapUtils.isEmpty(registry)) {
            registry = new ConcurrentHashMap<>();

            registerDataTemplate(tableTemplate);
        }
    }

    private void registerDataTemplate(NamedDataTemplate dataTemplate) {
        registry.put(dataTemplate.getName(), dataTemplate);
    }

}
