package com.latticeengines.datacloud.match.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

public interface AccountMasterColumnDao extends BaseDao<AccountMasterColumn> {
	List<AccountMasterColumn> findByTag(String tag);
}
