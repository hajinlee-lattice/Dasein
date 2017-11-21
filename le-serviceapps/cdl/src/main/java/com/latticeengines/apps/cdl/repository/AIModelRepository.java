package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.EntityGraph.EntityGraphType;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.AIModel;

public interface AIModelRepository extends BaseJpaRepository<AIModel, Long>{
	
	@EntityGraph(attributePaths = { "ratingEngine" })
	public AIModel findById(String id);
	
	@EntityGraph(value = "RatingModel.ratingEngine", type = EntityGraphType.LOAD)
	public AIModel findTopByRatingEngineIdOrderByCreatedDesc(String ratingEngineId);
	
	public List<AIModel> findByRatingEngineId(String ratingEngineId);
	
	public List<AIModel> findByRatingEngineId(String ratingEngineId, Pageable pageable);
	
}
