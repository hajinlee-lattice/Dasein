package com.latticeengines.domain.exposed.metadata.mds;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;

public abstract class MapDecorator implements Decorator, NeedsLoad {

    private static final Logger log = LoggerFactory.getLogger(MapDecorator.class);

    private final ConcurrentMap<String, ColumnMetadata> filterMap = new ConcurrentHashMap<>();

    private final String name;
    private AtomicBoolean loaded = new AtomicBoolean();

    protected MapDecorator(String name) {
        this.name = name;
    }

    public synchronized Mono<Boolean> load() {
        AtomicLong start = new AtomicLong();
        return Mono.fromCallable(() -> {
            loadInternal().forEach(cm -> filterMap.put(cm.getAttrName(), cm));
            return true;
        }).doOnSubscribe(subscription -> {
            loaded.set(true);
            start.set(System.currentTimeMillis());
            log.info(getLoggerName() + ": Start loading.");
        }).doOnSuccess(s -> {
            long duration = System.currentTimeMillis() - start.getAndIncrement();
            log.info(getLoggerName() + ": Loaded " + filterMap.size() + " filters in " + duration
                    + " msecs.");
        }).doOnError(t -> log.error(getLoggerName() + ": Failed to load.", t));
    }

    @Override
    public boolean isLoaded() {
        return loaded.get();
    }

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        AtomicLong counter = new AtomicLong();
        return metadata //
                .doOnSubscribe(s -> blockingLoad()) //
                .map(cm -> {
                    if (filterMap.containsKey(cm.getAttrName())) {
                        counter.incrementAndGet();
                        return ColumnMetadataUtils.overwrite(filterMap.get(cm.getAttrName()), cm);
                    } else {
                        return cm;
                    }
                }) //
                .doOnComplete(() -> {
                    if (counter.get() > 0) {
                        log.info(getLoggerName() + ": Rendered " + counter.get() + " attributes.");
                    }
                }) //
                .doOnError(t -> log.error(getLoggerName() + ": Failed to render.", t));
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        ThreadLocal<AtomicLong> counter = new ThreadLocal<>();
        return metadata //
                .doOnSubscribe(s -> blockingLoad()) //
                .map(cm -> {
                    if (counter.get() == null) {
                        counter.set(new AtomicLong());
                    }
                    if (filterMap.containsKey(cm.getAttrName())) {
                        counter.get().incrementAndGet();
                        return ColumnMetadataUtils.overwrite(filterMap.get(cm.getAttrName()), cm);
                    } else {
                        return cm;
                    }
                }) //
                .doOnComplete(() -> {
                    long count = 0;
                    if (counter.get() != null) {
                        count = counter.get().get();
                    }
                    if (count > 0) {
                        log.info(getLoggerName() + ": Rendered " + count + " attributes.");
                    }
                }) //
                .doOnError(t -> log.error(getLoggerName() + ": Failed to render.", t));
    }

    @Override
    public String getName() {
        return name;
    }

    private String getLoggerName() {
        String name = getName();
        if (StringUtils.isBlank(name)) {
            name = getClass().getSimpleName();
        }
        if (StringUtils.isBlank(name)) {
            name = getClass().getName().substring(getClass().getName().lastIndexOf(".") + 1);
        }
        return name;
    }

    // should be able to load all synchronously
    protected abstract Collection<ColumnMetadata> loadInternal();

}
