package com.latticeengines.metadata.mds.impl;

import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Id;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import com.latticeengines.documentdb.entity.MetadataEntity;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.metadata.repository.MetadataStoreRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class JpaRepositoryMetadataStore<T extends MetadataEntity> {

    private static final Logger log = LoggerFactory.getLogger(JpaRepositoryMetadataStore.class);

    protected abstract Class<T> getEntityClz();
    protected abstract MetadataStoreRepository<T> getRepository();

    private static Scheduler scheduler = Schedulers.newParallel("metadata-store");
    private String idField = null;

    public Flux<ColumnMetadata> getMetadata(String... namespace) {
        Class<T> clz = getEntityClz();
        MetadataStoreRepository<T> repository = getRepository();
        long count = repository.countByNameSpace(clz, namespace);
        if (count < 5000) {
            return Mono.fromCallable(() -> repository.findByNamespace(getEntityClz(), null, namespace)) //
                    .flatMapMany(Flux::fromIterable).map(MetadataEntity::getMetadata);
        } else {
            int pageSize = 2000;
            int pages = (int) Math.ceil(1.0 * count / pageSize);
            String idField = findIdField();
            return Flux.range(0, pages).parallel().runOn(scheduler).concatMap(page -> {
                PageRequest pageRequest = PageRequest.of(page, pageSize, Sort.by(idField));
                List<T> dataPage = repository.findByNamespace(clz, pageRequest, namespace);
                return Flux.fromIterable(dataPage);
            }).map(MetadataEntity::getMetadata).sequential();
        }
    }

    private String findIdField() {
        if (StringUtils.isBlank(idField)) {
            Class<?> c = getEntityClz();
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

}
