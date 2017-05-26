package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.DnbMatchCommandDao;
import com.latticeengines.datacloud.match.entitymgr.DnbMatchCommandEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;

@Component("dnbMatchCommandEntityMgr")
public class DnbMatchCommandEntityMgrImpl implements DnbMatchCommandEntityMgr {

    @Autowired
    private DnbMatchCommandDao dnbMatchCommandDao;

    @Override
    @Transactional(value = "propDataManage")
    public void createCommand(DnBMatchCommand record) {
        dnbMatchCommandDao.create(record);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void updateCommand(DnBMatchCommand record) {
        dnbMatchCommandDao.update(record);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void deleteCommand(DnBMatchCommand record) {
        dnbMatchCommandDao.delete(record);
    }

    @Override
    @Transactional(value = "propDataManage")
    public DnBMatchCommand findRecordByField(String field, Object value) {
        return dnbMatchCommandDao.findByField(field, value);
    }

    @Override
    @Transactional(value = "propDataManage")
    public List<DnBMatchCommand> findAllByField(String field, Object value) {
        return dnbMatchCommandDao.findAllByField(field, value);
    }

    @Override
    @Transactional(value = "propDataManage")
    public void abandonCommands(String rootOperationUid) {
        dnbMatchCommandDao.abandonCommands(rootOperationUid);
    }
}
