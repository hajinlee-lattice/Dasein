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
import com.latticeengines.metadata.repository.document.MetadataStoreRepository;
import com.latticeengines.common.exposed.timer.PerformanceTimer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class JpaRepositoryMetadataStore<T extends ColumnMetadataDocument> {

    protected abstract Class<T> getEntityClz();
    protected abstract MetadataStoreRepository<T> getRepository();
    private static final int PAGE_SIZE = 5000;

    private Scheduler jpaScheduler = Schedulers.newParallel("jpa-mds");
    private String idField = null;

    protected Long count(Serializable... namespace) {
        Class<T> clz = getEntityClz();
        MetadataStoreRepository<T> repository = getRepository();
        return repository.countByNameSpace(clz, namespace);
    }

    protected Flux<ColumnMetadata> getMetadata(Serializable... namespace) {
        Class<T> clz = getEntityClz();
        MetadataStoreRepository<T> repository = getRepository();
        long count = repository.countByNameSpace(clz, namespace);
        if (count < 2 * PAGE_SIZE) {
            return Mono.fromCallable(() -> repository.findByNamespace(getEntityClz(), null, namespace)) //
                    .flatMapMany(Flux::fromIterable).map(ColumnMetadataDocument::getColumnMetadata);
        } else {
            int pages = (int) Math.ceil(1.0 * count / PAGE_SIZE);
            String idField = findIdField();
            return Flux.range(0, pages) //
                    .parallel().runOn(jpaScheduler) //
                    .concatMap(page -> {
                        try (PerformanceTimer timer = new PerformanceTimer()) {
                            PageRequest pageRequest = PageRequest.of(page, PAGE_SIZE, Sort.by(idField));
                            List<T> dataPage = repository.findByNamespace(clz, pageRequest, namespace);
                            timer.setTimerMessage("Fetched a page of " + dataPage.size() + " records.");
                            return Flux.fromIterable(dataPage);
                        }
                    }).map(ColumnMetadataDocument::getColumnMetadata).sequential();
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
