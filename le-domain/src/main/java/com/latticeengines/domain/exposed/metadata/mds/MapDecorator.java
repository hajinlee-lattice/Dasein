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
import reactor.core.publisher.ParallelFlux;

public abstract class MapDecorator implements Decorator, NeedsLoad {

    private static final Logger log = LoggerFactory.getLogger(MapDecorator.class);

    private final ConcurrentMap<String, ColumnMetadata> filterMap = new ConcurrentHashMap<>();

    private final String name;
    private AtomicBoolean loaded = new AtomicBoolean();

    protected MapDecorator(String name) {
        this.name = name;
    }

    public synchronized void load() {
        AtomicLong start = new AtomicLong();
        log.info(getLoggerName() + ": Start loading.");
        Collection<ColumnMetadata> cms = loadInternal();
        cms.forEach(cm -> filterMap.put(cm.getAttrName(), cm));
        long duration = System.currentTimeMillis() - start.getAndIncrement();
        log.info(getLoggerName() + ": Loaded " + filterMap.size() + " filters in " + duration
                + " msecs.");
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
                    cm = preProcess(cm);
                    if (filterMap.containsKey(cm.getAttrName())) {
                        counter.incrementAndGet();
                        cm = ColumnMetadataUtils.overwrite(filterMap.get(cm.getAttrName()), cm);
                    }
                    return postProcess(cm);
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
                    cm = preProcess(cm);
                    if (filterMap.containsKey(cm.getAttrName())) {
                        counter.get().incrementAndGet();
                        cm = ColumnMetadataUtils.overwrite(filterMap.get(cm.getAttrName()), cm);
                    }
                    return postProcess(cm);
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

    protected ColumnMetadata preProcess(ColumnMetadata cm) {
        return cm;
    }

    protected ColumnMetadata postProcess(ColumnMetadata cm) {
        return cm;
    }

}
