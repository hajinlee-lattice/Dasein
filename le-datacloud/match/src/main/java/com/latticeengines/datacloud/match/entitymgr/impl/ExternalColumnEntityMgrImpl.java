package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.match.dao.ExternalColumnDao;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.repository.reader.ExternalColumnReaderRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;

@Component("externalColumnEntityMgr")
public class ExternalColumnEntityMgrImpl extends BaseEntityMgrRepositoryImpl<ExternalColumn, Long>
        implements MetadataColumnEntityMgr<ExternalColumn> {

    @Resource(name = "externalColumnDao")
    private ExternalColumnDao externalColumnDao;

    @Inject
    private ExternalColumnReaderRepository repository;

    @Override
    public BaseDao<ExternalColumn> getDao() {
        return externalColumnDao;
    }

    @Override
    public BaseJpaRepository<ExternalColumn, Long> getRepository() {
        return repository;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @VisibleForTesting
    public void create(ExternalColumn externalColumn) {
        externalColumnDao.create(externalColumn);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> findByTag(String tag, String dataCloudVersion) {
        List<ExternalColumn> columns = externalColumnDao.findByTag(tag);
        List<ExternalColumn> toReturn = new ArrayList<>();
        for (ExternalColumn column : columns) {
            if (column.getTagList().contains(tag)) {
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ParallelFlux<ExternalColumn> findAll(String dataCloudVersion) {
        return Mono.fromCallable(() -> repository.findAll()).flatMapMany(Flux::fromIterable)
                .parallel().runOn(Schedulers.newParallel("ext-col"));
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ExternalColumn findById(String externalColumnId, String dataCloudVersion) {
        return externalColumnDao.findByField("ExternalColumnID", externalColumnId);
    }

    @Override
    public Flux<ExternalColumn> findByPage(String dataCloudVersion, int page, int pageSize) {
        return Mono.fromCallable(() -> {
            PageRequest pageRequest = PageRequest.of(page, pageSize, Sort.by("externalColumnID"));
            return repository.findAll(pageRequest);
        }).flatMapMany(Flux::fromIterable);
    }

    @Override
    public Long count(String dataCloudVersion) {
        return repository.count();
    }

}
