package com.latticeengines.metadata.mds.impl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.NotImplementedException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplateName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.metadata.mds.NamedDataTemplate;
import com.latticeengines.metadata.repository.db.AttributeRepository;
import com.latticeengines.metadata.repository.db.TableRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;

@Component("tableTemplate")
public class TableTemplate implements NamedDataTemplate<Namespace1<Long>> {

    private static final int PAGE_SIZE = 5000;

    @Inject
    private AttributeRepository repository;

    @Inject
    private TableRepository tableRepository;

    @Override
    public String getName() {
        return DataTemplateName.Table;
    }

    @Override
    public List<DataUnit> getData(Namespace1<Long> namespace) {
        throw new NotImplementedException("Data unit framework has not been implemented yet.");
    }

    @Override
    public long countSchema(Namespace1<Long> namespace) {
        long tablePid = namespace.getCoord1();
        return repository.countByTable_Pid(tablePid);
    }

    @Override
    public Flux<ColumnMetadata> getSchema(Namespace1<Long> namespace) {
        long tablePid = namespace.getCoord1();
        long count = getCount(tablePid);
        if (count < 2 * PAGE_SIZE) {
            return Mono.fromCallable(() -> {
                try (PerformanceTimer timer = new PerformanceTimer()) {
                    List<Attribute> attrs = repository.findByTable_Pid(tablePid);
                    timer.setTimerMessage("Fetched all " + attrs.size() + " attributes.");
                    return attrs;
                }
            }).flatMapMany(Flux::fromIterable).map(Attribute::getColumnMetadata);
        } else {
            int pages = (int) Math.ceil(1.0 * count / PAGE_SIZE);
            return parallelQuery(pages, tablePid).sorted(Comparator.comparing(Attribute::getPid)) //
                    .map(Attribute::getColumnMetadata);
        }
    }

    @Override
    public ParallelFlux<ColumnMetadata> getUnorderedSchema(Namespace1<Long> namespace) {
        long tablePid = namespace.getCoord1();
        long count = getCount(tablePid);
        int pages = (int) Math.ceil(1.0 * count / PAGE_SIZE);
        return parallelQuery(pages, tablePid).map(Attribute::getColumnMetadata);
    }

    private long getCount(long tablePid) {
        return repository.countByTable_Pid(tablePid);
    }

    private ParallelFlux<Attribute> parallelQuery(int pages, Long tablePid) {
        return Flux.range(0, pages) //
                .parallel().runOn(scheduler) //
                .concatMap(page -> Flux.fromIterable(queryPage(tablePid, page)));
    }

    private List<Attribute> queryPage(long tablePid, int page) {
        try (PerformanceTimer timer = new PerformanceTimer()) {
            PageRequest pageRequest = PageRequest.of(page, PAGE_SIZE);
            List<Attribute> dataPage = repository.findByTable_Pid(tablePid, pageRequest);
            timer.setTimerMessage("Fetched a page of " + dataPage.size() + " attributes.");
            return dataPage;
        }
    }

    // tenantId, tableName -> tablePid
    @Override
    public Namespace1<Long> parseNameSpace(String... namespace) {
        if (namespace.length != 2) {
            throw new IllegalArgumentException("The namespace for " + getName()
                    + " should have two coordinates, but found " + Arrays.toString(namespace));
        }
        String tenantId = CustomerSpace.parse(namespace[0]).toString();
        String tableName = namespace[1];
        Long tablePid = tableRepository.findPidByTenantIdAndName(tenantId, tableName);
        if (tablePid == null) {
            throw new IllegalArgumentException("Cannot find table named " + tableName + " in tenant " + namespace[0]);
        }
        return Namespace.as(tablePid);
    }
}
