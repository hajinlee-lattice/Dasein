package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.dao.PatchBookDao;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook.Type;

@Component("patchBookEntityMgr")
public class PatchBookEntityMgrImpl extends BaseEntityMgrImpl<PatchBook> implements PatchBookEntityMgr {

    @Inject
    private PatchBookDao patchBookDao;

    @Override
    public PatchBookDao getDao() {
        return patchBookDao;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    public List<PatchBook> findByType(@NotNull PatchBook.Type type) {
        Preconditions.checkNotNull(type);
        return getDao().findAllByField(PatchBook.COLUMN_TYPE, type.name());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    public List<PatchBook> findByTypeAndHotFix(@NotNull PatchBook.Type type, boolean hotFix) {
        Preconditions.checkNotNull(type);
        return getDao().findAllByFields(PatchBook.COLUMN_TYPE, type.name(), PatchBook.COLUMN_HOTFIX,
                hotFix);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    public List<PatchBook> findByTypeWithPagination(@NotNull int offset, @NotNull int limit,
            @NotNull String sortByField, PatchBook.Type type) {
        return getDao().findAllSortedByFieldWithPagination(offset, limit, sortByField,
                PatchBook.COLUMN_TYPE, type.name());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    public List<PatchBook> findByTypeAndHotFixWithPagination(@NotNull int offset, @NotNull int limit,
            @NotNull String sortByField, PatchBook.Type type, boolean hotfix) {
        return getDao().findAllSortedByFieldWithPagination(offset, limit, sortByField, PatchBook.COLUMN_TYPE,
                type.name(), PatchBook.COLUMN_HOTFIX, hotfix);
    }

    /* override methods that require write access to use the correct manager (write connection) */

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void create(PatchBook entity) {
        super.create(entity);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void update(PatchBook entity) {
        super.update(entity);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void createOrUpdate(PatchBook entity) {
        super.createOrUpdate(entity);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void delete(PatchBook entity) {
        super.delete(entity);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void deleteAll() {
        super.deleteAll();
    }

    /* methods to set a value to a field for all PatchBooks in the given list */

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void setHotFix(List<Long> pIds, boolean hotFix) {
        getDao().updateField(pIds, PatchBook.COLUMN_HOTFIX, hotFix);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void setEndOfLife(List<Long> pIds, boolean endOfLife) {
        getDao().updateField(pIds, PatchBook.COLUMN_EOL, endOfLife);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void setEffectiveSinceVersion(List<Long> pIds, String version) {
        getDao().updateField(pIds, PatchBook.COLUMN_EFFECTIVE_SINCE_VERSION, version);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void setExpireAfterVersion(List<Long> pIds, String version) {
        getDao().updateField(pIds, PatchBook.COLUMN_EXPIRE_AFTER_VERSION, version);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    public List<PatchBook> findByTypeWithPagin(@NotNull long minPid, @NotNull long maxPid,
            Type type) {
        return getDao().findByFieldsWithPagination(minPid, maxPid, PatchBook.COLUMN_TYPE,
                type.name());
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED, readOnly = true)
    public List<PatchBook> findByTypeAndHotFixWithPagin(@NotNull long minPid, @NotNull long maxPid,
            Type type, boolean hotfix) {
        return getDao().findByFieldsWithPagination(minPid, maxPid, PatchBook.COLUMN_TYPE,
                type.name(), PatchBook.COLUMN_HOTFIX, hotfix);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public Map<String, Long> findMinMaxPid(Type type, String pidColumn) {
        return getDao().getMinMaxPid(type, PatchBook.COLUMN_PID);
    }

}
