package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelWithPublishedHistoryDTO;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.ratings.coverage.ProductsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;

public interface RatingEngineService {

    List<RatingEngineSummary> getRatingEngineSummaries(String status, String type, Boolean publishedRatingsOnly);

    List<RatingEngine> getAllDeletedRatingEngines();

    RatingEngineSummary getRatingEngineSummary(String ratingEngineId);

    RatingEngine getRatingEngine(String ratingEngineId);

    DataPage getEntityPreview(String ratingEngineId, long offset, long maximum, BusinessEntity entityType,
                              String sortBy, String bucketFieldName, Boolean descending, List<String> lookupFieldNames,
                              Boolean restrictNotNullSalesforceId, String freeFormTextSearch, String freeFormTextSearch2,
                              List<String> selectedBuckets, String lookupIdColumn);

    Long getEntityPreviewCount(String ratingEngineId, BusinessEntity entityType, Boolean restrictNotNullSalesforceId,
                               String freeFormTextSearch, List<String> selectedBuckets, String lookupIdColumn);

    RatingEngineDashboard getRatingEngineDashboardById(String ratingEngineId);

    List<RatingModelWithPublishedHistoryDTO> getPublishedHistory(String ratingEngineId);

    RatingEngine createOrUpdateRatingEngine(RatingEngine ratingEngine, Boolean unlinkSegment, Boolean createAction);

    Boolean deleteRatingEngine(String ratingEngineId, Boolean hardDelete);

    Boolean revertDeleteRatingEngine(String ratingEngineId);

    RatingsCountResponse getRatingEngineCoverageInfo(RatingsCountRequest ratingModelSegmentIds);

    RatingEnginesCoverageResponse getRatingEngineCoverageInfo(String segmentName,
                                                              RatingEnginesCoverageRequest ratingEnginesCoverageRequest);

    RatingEnginesCoverageResponse getProductCoverageInfoForSegment(Integer purchasedBeforePeriod,
                                                                   ProductsCoverageRequest productsCoverageRequest);

    List<RatingModel> getRatingModels(String ratingEngineId);

    RatingModel createModelIteration(String ratingEngineId, RatingModel ratingModel);

    RatingModel getRatingModel(String ratingEngineId, String ratingModelId);

    RatingModel updateRatingModel(String ratingEngineId, String ratingModelId, RatingModel ratingModel);

    List<ColumnMetadata> getIterationMetadata(String ratingEngineId, String ratingModelId, String dataStores);

    Map<String, StatsCube> getIterationMetadataCube(String ratingEngineId, String ratingModelId, String dataStores);

    TopNTree getIterationMetadataTopN(String ratingEngineId, String ratingModelId, String dataStores);

    void setScoringIteration(String ratingEngineId, String ratingModelId, List<BucketMetadata> bucketMetadatas);

    List<RatingEngineNote> getAllNotes(String ratingEngineId);

    Map<String, List<String>> getRatingEngineDependencies(String ratingEngineId);

    Map<String, List<String>> getRatingEnigneDependenciesModelAndView(String ratingEngineId);

    Boolean createNote(String ratingEngineId, NoteParams noteParams);

    void deleteNote(String ratingEngineId, String noteId);

    Boolean updateNote(String ratingEngineId, String noteId, NoteParams noteParams);

    EventFrontEndQuery getModelingQuery(String ratingEngineId, String ratingModelId,
                                        ModelingQueryType modelingQueryType, RatingEngine ratingEngine);

    Long getModelingQueryCount(String ratingEngineId, String ratingModelId, ModelingQueryType modelingQueryType,
                               RatingEngine ratingEngine);

    EventFrontEndQuery getModelingQueryByRatingId(String ratingEngineId, String ratingModelId,
                                                  ModelingQueryType modelingQueryType);

    Long getModelingQueryCountByRatingId(String ratingEngineId, String ratingModelId, ModelingQueryType modelingQueryType);

    String ratingEngineModel(String ratingEngineId, String ratingModelId, List<ColumnMetadata> attributes, boolean skipValidation);

    boolean validateForModeling(String ratingEngineId, String ratingModelId);

    boolean validateForModeling(String ratingEngineId, String ratingModelId, RatingEngine ratingEngine);
}
