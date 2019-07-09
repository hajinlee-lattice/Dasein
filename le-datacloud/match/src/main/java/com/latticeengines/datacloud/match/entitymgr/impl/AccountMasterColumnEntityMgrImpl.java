package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.match.dao.AccountMasterColumnDao;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.repository.reader.AccountMasterColumnReaderRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Component("accountMasterColumnEntityMgr")
public class AccountMasterColumnEntityMgrImpl extends BaseEntityMgrRepositoryImpl<AccountMasterColumn, Long>
        implements MetadataColumnEntityMgr<AccountMasterColumn> {

    private static final Logger log = LoggerFactory.getLogger(AccountMasterColumnEntityMgrImpl.class);

    private static final int PAGE_SIZE = 10000;

    private Scheduler scheduler;

    @Resource(name = "accountMasterColumnDao")
    private AccountMasterColumnDao accountMasterColumnDao;

    @Inject
    private AccountMasterColumnReaderRepository repository;

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
    public ParallelFlux<AccountMasterColumn> findAll(String dataCloudVersion) {
        long count;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            RetryTemplate retry = RetryUtils.getRetryTemplate(5);
            count = retry.execute(ctx -> repository.numAttrsInVersion(dataCloudVersion));
            String msg = "Got the count of AMColumns for version " + dataCloudVersion + ": " + count;
            timer.setTimerMessage(msg);
        }
        int pages = (int) Math.ceil(1.0 * count / PAGE_SIZE);

        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        if (BeanFactoryEnvironment.Environment.AppMaster.equals(currentEnv)) {
            // in yarn container
            return findAll(dataCloudVersion, pages);
        }

        return Flux.range(0, pages).parallel().runOn(getScheduler()) //
                .map(k -> {
                    try (PerformanceTimer timer = new PerformanceTimer()) {
                        PageRequest pageRequest = PageRequest.of(k, PAGE_SIZE, Sort.by("amColumnId"));
                        RetryTemplate retry = RetryUtils.getRetryTemplate(5);
                        List<AccountMasterColumn> attrs = retry.execute(ctx -> {
                            if (ctx.getRetryCount() > 0) {
                                log.info("Attempt=" + (ctx.getRetryCount() + 1) + ": get " //
                                        + k + "-th page of AM metadata.");
                            }
                            return repository.findByDataCloudVersion(dataCloudVersion, pageRequest);
                        });
                        timer.setTimerMessage("Fetched a page of " + attrs.size() + " AM attrs.");
                        return attrs;
                    }
                }).flatMap(Flux::fromIterable);
    }

    /*
     * Retrieve each page of AM metadata sequentially
     */
    private ParallelFlux<AccountMasterColumn> findAll(String dataCloudVersion, int nPages) {
        List<AccountMasterColumn> attrs = new ArrayList<>();
        // start backoff at 15s, grows 2x (total wait time ~ 30min)
        RetryTemplate retry = RetryUtils.getExponentialBackoffRetryTemplate(7, 15000, 2.0D, null);
        for (int i = 0; i < nPages; i++) {
            int page = i;
            try (PerformanceTimer timer = new PerformanceTimer()) {
                attrs.addAll(retry.execute(ctx -> {
                    if (ctx.getRetryCount() > 0) {
                        log.info("Attempt #{} to get {}-th page of AM metadata", ctx.getRetryCount() + 1, page);
                    }
                    PageRequest pageRequest = PageRequest.of(page, PAGE_SIZE, Sort.by("amColumnId"));
                    return repository.findByDataCloudVersion(dataCloudVersion, pageRequest);
                }));
                timer.setTimerMessage(String.format("Fetch page #%d. Total AM attrs = %d", page, attrs.size()));
            }
        }
        return ParallelFlux.from(Flux.fromIterable(attrs));
    }

    @Override
    public Flux<AccountMasterColumn> findByPage(String dataCloudVersion, int page, int pageSize) {
        return Mono.fromCallable(() -> {
            RetryTemplate retry = RetryUtils.getRetryTemplate(5);
            PageRequest pageRequest = PageRequest.of(page, pageSize, Sort.by("amColumnId"));
            return retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt=" + (ctx.getRetryCount() + 1) + ": get a page of AM metadata.");
                }
                return repository.findByDataCloudVersion(dataCloudVersion, pageRequest);
            });
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

    private Scheduler getScheduler() {
        if (scheduler == null) {
            synchronized (this) {
                if (scheduler == null) {
                    scheduler = Schedulers.newParallel("am-metadata");
                }
            }
        }
        return scheduler;
    }

}
