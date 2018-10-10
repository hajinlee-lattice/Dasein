package com.latticeengines.datacloud.match.entitymgr.impl;

import com.latticeengines.datacloud.match.dao.PatchBookDao;
import com.latticeengines.datacloud.match.entitymgr.PatchBookEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.inject.Inject;
import java.util.List;

@Component("patchBookEntityMgr")
public class PatchBookEntityMgrImpl extends BaseEntityMgrImpl<PatchBook> implements PatchBookEntityMgr {

    @Inject
    private PatchBookDao patchBookDao;

    @Override
    public PatchBookDao getDao() {
        return patchBookDao;
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
}