package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.match.dao.AccountMasterColumnDao;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.repository.AccountMasterColumnRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("accountMasterColumnEntityMgr")
public class AccountMasterColumnEntityMgrImpl
        extends BaseEntityMgrRepositoryImpl<AccountMasterColumn, Long>
        implements MetadataColumnEntityMgr<AccountMasterColumn> {

    private static final int PAGE_SIZE = 10000;
    private static final Scheduler scheduler = Schedulers.newParallel("am-metadata");

    @Resource(name = "accountMasterColumnDao")
    private AccountMasterColumnDao accountMasterColumnDao;

    @Inject
    private AccountMasterColumnRepository repository;

    @Override
    public BaseDao<AccountMasterColumn> getDao() {
        return accountMasterColumnDao;
    }

    @Override
    public BaseJpaRepository<AccountMasterColumn, Long> getRepository() {
        return repository;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @VisibleForTesting
    public void create(AccountMasterColumn accountMasterColumn) {
        accountMasterColumnDao.create(accountMasterColumn);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AccountMasterColumn> findByTag(String tag, String dataCloudVersion) {
        return accountMasterColumnDao.findByTag(tag, dataCloudVersion);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ParallelFlux<AccountMasterColumn> findAll(String dataCloudVersion) {
        long count;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            count = repository.countByDataCloudVersion(dataCloudVersion);
            String msg = "Got the count of AMColumns for version " + dataCloudVersion + ": " + count;
            timer.setTimerMessage(msg);
        }
        int pages = (int) Math.ceil(1.0 * count / PAGE_SIZE);
        return Flux.range(0, pages).parallel().runOn(scheduler) //
                .map(k -> {
                    try (PerformanceTimer timer = new PerformanceTimer()) {
                        PageRequest pageRequest = PageRequest.of(k, PAGE_SIZE, Sort.by("amColumnId"));
                        List<AccountMasterColumn> attrs = repository.findByDataCloudVersion(dataCloudVersion, pageRequest);
                        timer.setTimerMessage("Fetched a page of " + attrs.size() + " AM attrs.");
                        return attrs;
                    }
                }).flatMap(Flux::fromIterable);
    }

    @Override
    public Flux<AccountMasterColumn> findByPage(String dataCloudVersion, int page, int pageSize) {
        return Mono.fromCallable(() -> {
            PageRequest pageRequest = PageRequest.of(page, pageSize, Sort.by("amColumnId"));
            return repository.findByDataCloudVersion(dataCloudVersion, pageRequest);
        }).flatMapMany(Flux::fromIterable);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AccountMasterColumn findById(String amColumnId, String dataCloudVersion) {
        return accountMasterColumnDao.findById(amColumnId, dataCloudVersion);
    }

    @Override
    public Long count(String dataCloudVersion) {
        return repository.countByDataCloudVersion(dataCloudVersion);
    }

}
