package com.latticeengines.apps.lp.repository.writer;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.AIModel;

public interface AIModelRepository extends BaseJpaRepository<AIModel, Long> {

    AIModel findByPid(Long pid);

}
