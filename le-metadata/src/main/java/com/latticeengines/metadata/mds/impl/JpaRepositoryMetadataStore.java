package com.latticeengines.metadata.mds.impl;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Id;

import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import com.latticeengines.documentdb.entity.ColumnMetadataDocument;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.metadata.repository.MetadataStoreRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class JpaRepositoryMetadataStore<T extends ColumnMetadataDocument> {

    protected abstract Class<T> getDAOClz();
    protected abstract MetadataStoreRepository<T> getRepository();

    private static Scheduler scheduler = Schedulers.newParallel("metadata-store");
    private String idField = null;

    public Flux<ColumnMetadata> getMetadata(Serializable... namespace) {
        Class<T> clz = getDAOClz();
        MetadataStoreRepository<T> repository = getRepository();
        long count = repository.countByNameSpace(clz, namespace);
        if (count < 5000) {
            return Mono.fromCallable(() -> repository.findByNamespace(clz, null, namespace)) //
                    .flatMapMany(Flux::fromIterable).map(ColumnMetadataDocument::getColumnMetadata);
        } else {
            int pageSize = 2000;
            int pages = (int) Math.ceil(1.0 * count / pageSize);
            String idField = findIdField();
            return Flux.range(0, pages).parallel().runOn(scheduler).concatMap(page -> {
                PageRequest pageRequest = PageRequest.of(page, pageSize, Sort.by(idField));
                List<T> dataPage = repository.findByNamespace(clz, pageRequest, namespace);
                return Flux.fromIterable(dataPage);
            }).map(ColumnMetadataDocument::getColumnMetadata).sequential();
        }
    }

    private String findIdField() {
        if (StringUtils.isBlank(idField)) {
            Class<?> c = getDAOClz();
            while (c != null) {
                for (Field field : c.getDeclaredFields()) {
                    if (field.isAnnotationPresent(Id.class)) {
                        idField = field.getName();
                        return idField;
                    }
                }
                c = c.getSuperclass();
            }
            idField = "uuid";
        }
        return idField;
    }

    // default impl assumes all namespace keys are string
    public Serializable[] parseNameSpace(String... namespace) {
        return namespace;
    }

}
