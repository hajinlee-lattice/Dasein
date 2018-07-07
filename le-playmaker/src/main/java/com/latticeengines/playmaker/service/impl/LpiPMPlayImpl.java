package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component("lpiPMPlay")
public class LpiPMPlayImpl implements LpiPMPlay {

    private static final Logger log = LoggerFactory.getLogger(LpiPMPlayImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private EntityProxy entityProxy;

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds) {
        List<Play> plays = getPlayList(start, playgroupIds);
        return handlePagination(start, offset, maximum, plays);
    }

    private List<Play> getPlayList(long start, List<Integer> playgroupIds) {
        // following API has sub-second performance even for large number of
        // plays (50+). This API returns only those plays for with there is
        // at least one play launch. Implementing pagination in this API may not
        // be worth the effort as it is fast enough. Therefore handling
        // pagination in application layer itself
        PlayLaunchDashboard dashboard = playProxy.getPlayLaunchDashboard(
                MultiTenantContext.getCustomerSpace().toString(), null, null, 0L, 0L, 1L, null, null, null, null, null);
        List<Play> plays = dashboard.getUniquePlaysWithLaunches();
        return plays;
    }

    private List<Map<String, Object>> getAllProducts() {
        FrontEndQuery query = new FrontEndQuery();
        query.setAccountRestriction(null);
        query.setContactRestriction(null);
        query.addLookups(BusinessEntity.Product, InterfaceName.ProductId.name());
        query.addLookups(BusinessEntity.Product, InterfaceName.ProductName.name());
        query.setMainEntity(BusinessEntity.Product);

        return entityProxy.getData(MultiTenantContext.getCustomerSpace().toString(), query).getData();
    }

    private List<Map<String, Object>> handlePagination(long start, int offset, int maximum, List<Play> plays) {
        List<Map<String, Object>> allProducts = getAllProducts();
        List<Map<String, Object>> result = new ArrayList<>();
        int skipped = 0;
        int rowNum = offset + 1;

        for (Play play : plays) {
            if (secondsFromEpoch(play) >= start) {
                if (skipped < offset) {
                    skipped++;
                    continue;
                }
                if (result.size() >= maximum) {
                    break;
                }
                generatePlayObjForSync(result, rowNum++, play, allProducts);
            }
        }
        return result;
    }

    private Map<String, Object> generatePlayObjForSync(List<Map<String, Object>> result, int rowNum, Play play,
            List<Map<String, Object>> allProducts) {
        Map<String, Object> playMap = new HashMap<>();
        playMap.put(PlaymakerConstants.ID, play.getPid());
        playMap.put(PlaymakerConstants.ID + PlaymakerConstants.V2, play.getName());
        playMap.put(PlaymakerConstants.ExternalId, play.getName());
        playMap.put(PlaymakerConstants.DisplayName, play.getDisplayName());
        playMap.put(PlaymakerConstants.Description, play.getDescription());
        playMap.put(PlaymakerConstants.AverageProbability, null);
        playMap.put(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY, secondsFromEpoch(play));
        playMap.put(PlaymakerConstants.PlayGroups, null);
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(MultiTenantContext.getCustomerSpace().toString(),
                play.getRatingEngine().getId());
        playMap.put(PlaymakerConstants.TargetProducts, getTargetProducts(ratingEngine, allProducts));
        playMap.put(PlaymakerConstants.Workflow, getPlayWorkFlowType(ratingEngine));

        playMap.put(PlaymakerConstants.RowNum, rowNum);
        result.add(playMap);
        return playMap;
    }

    private String getPlayWorkFlowType(RatingEngine ratingEngine) {
        final String defaultValue = PlaymakerConstants.DefaultWorkflowType; // RulesBased
        if (ratingEngine == null) {
            return defaultValue;
        }
        try {
            switch (ratingEngine.getType()) {
            case RULE_BASED:
                return defaultValue;
            case CUSTOM_EVENT:
            case PROSPECTING:
                return "Prospecting";
            case CROSS_SELL:
                ModelingStrategy strategy = ((CrossSellModelingConfig) ((AIModel) ratingEngine.getPublishedIteration())
                        .getAdvancedModelingConfig()).getModelingStrategy();
                if (strategy == ModelingStrategy.CROSS_SELL_FIRST_PURCHASE) {
                    return "Cross-Sell";
                }
                if (strategy == ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE) {
                    return "Upsell";
                }
            default:
                return defaultValue;
            }
        } catch (Exception e) {
            log.error("Failed to translate the Rating Engine into the Play's WorkFlowType for RatingEngine: "
                    + ratingEngine.getId(), e);
            return defaultValue;
        }
    }

    // TODO - remove following when we have products associated with plays
    private List<Map<String, String>> getTargetProducts(RatingEngine ratingEngine,
            List<Map<String, Object>> allProducts) {
        if (ratingEngine == null) {
            return null;
        }
        if (ratingEngine.getType() != RatingEngineType.CROSS_SELL) {
            return null;
        }

        List<String> targetProductIds = ((CrossSellModelingConfig) ((AIModel) ratingEngine.getPublishedIteration())
                .getAdvancedModelingConfig()).getTargetProducts();
        List<Map<String, String>> toReturn = new ArrayList<>();

        targetProductIds.forEach(productId -> {
            Map<String, Object> foundProduct = findProductById(productId, allProducts);
            if (foundProduct != null) {
                HashMap<String, String> prod = new HashMap<>();
                prod.put(PlaymakerConstants.DisplayName, (String) foundProduct.get(InterfaceName.ProductName.name()));
                prod.put(PlaymakerConstants.ExternalName, (String) foundProduct.get(InterfaceName.ProductId.name()));
                toReturn.add(prod);
            }
        });
        return toReturn;
    }

    private Map<String, Object> findProductById(String toFind, List<Map<String, Object>> allProducts) {
        try {
            return allProducts.stream().filter(prod -> prod.get(InterfaceName.ProductId.name()).equals(toFind))
                    .findFirst().get();
        } catch (NoSuchElementException e) {
            log.error("Unable to find the product id: " + toFind);
            return null;
        } catch (Exception e) {
            log.error("Failed to find the product id: " + toFind);
            return null;
        }
    }

    @VisibleForTesting
    long secondsFromEpoch(Play play) {
        try {
            return play.getUpdated().getTime() / 1000;
        } catch (Exception ex) {
            log.error("Ignoring this error", ex);
            return 0L;
        }
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        List<Play> plays = getPlayList(start, playgroupIds);
        int count = 0;
        if (CollectionUtils.isNotEmpty(plays)) {
            count = new Long( //
                    plays.stream() //
                            .filter(p -> secondsFromEpoch(p) >= start) //
                            .count()) //
                                    .intValue();
        }

        return count;
    }

    @VisibleForTesting
    void setPlayProxy(PlayProxy playProxy) {
        this.playProxy = playProxy;
    }

}
