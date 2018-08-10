package com.latticeengines.apps.cdl.repository.writer;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.EntityGraph.EntityGraphType;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.AIModel;

public interface AIModelRepository extends BaseJpaRepository<AIModel, Long> {

    @EntityGraph(attributePaths = { "ratingEngine", "trainingSegment" })
    AIModel findById(String id);

    @EntityGraph(value = "AIModel.details", type = EntityGraphType.LOAD)
    AIModel findTopByRatingEngineIdOrderByCreatedDesc(String ratingEngineId);

    List<AIModel> findAllByRatingEngineId(String ratingEngineId);

    List<AIModel> findAllByRatingEngineId(String ratingEngineId, Pageable pageable);

}
