package com.latticeengines.apps.lp.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;

public interface ModelFeatureImportanceWriterRepository extends BaseJpaRepository<ModelFeatureImportance, Long> {

    List<ModelFeatureImportance> findByModelSummary_Id(String modelGuid);

}
