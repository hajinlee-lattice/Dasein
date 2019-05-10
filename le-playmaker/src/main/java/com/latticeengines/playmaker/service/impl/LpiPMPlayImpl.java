package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.LookupIdMapUtils;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayGroup;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayType;
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
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds,
            int syncDestination, Map<String, String> orgInfo) {
        List<Play> plays = getPlayList(start, playgroupIds, syncDestination, orgInfo);
        return handlePagination(start, offset, maximum, plays);
    }

    @Override
    public List<Play> getPlayList(long start, List<Integer> playgroupIds, int syncDestination,
            Map<String, String> orgInfo) {
        // following API has sub-second performance even for large number of
        // plays (50+). This API returns only those plays for with there is
        // at least one play launch. Implementing pagination in this API may not
        // be worth the effort as it is fast enough. Therefore handling
        // pagination in application layer itself
        List<Play> plays;
        List<LaunchState> launchstates = new ArrayList<>();
        launchstates.add(LaunchState.Launched);
        if (start < 90000000000L) { // if request using second level timestamp
            start = start * 1000L;
        }
        PlayLaunchDashboard dashboard;
        if (orgInfo == null) {
            dashboard = playProxy.getPlayLaunchDashboard(MultiTenantContext.getCustomerSpace().toString(), null,
                    launchstates, start, 0L, 1000L, null, null, null, null, null);
        } else {
            Pair<String, String> effectiveOrgInfo = LookupIdMapUtils.getEffectiveOrgInfo(orgInfo);
            dashboard = playProxy.getPlayLaunchDashboard(MultiTenantContext.getCustomerSpace().toString(), null,
                    launchstates, start, 0L, 1000L, null, null, null, effectiveOrgInfo.getLeft(),
                    effectiveOrgInfo.getRight());
        }
        plays = dashboard.getUniquePlaysWithLaunches();
        return plays;
    }

    @Override
    public List<String> getLaunchIdsFromDashboard(boolean latest, long start, List<String> playIds, int syncDestination,
            Map<String, String> orgInfo) {
        List<LaunchSummary> summaries = getLaunchSummariesFromDashboard(latest, start, playIds, syncDestination, orgInfo);
        if (CollectionUtils.isEmpty(summaries)) {
            return Collections.EMPTY_LIST;
        }
        return summaries.stream().map(launchSummary -> launchSummary.getLaunchId()).collect(Collectors.toList());
    }

    @Override
    public List<LaunchSummary> getLaunchSummariesFromDashboard(boolean latest, long start, List<String> playIds,
            int syncDestination, Map<String, String> orgInfo) {

        List<LaunchSummary> filteredLaunchSummaries = new ArrayList<>();
        PlayLaunchDashboard dashboard;
        List<LaunchState> launchstates = new ArrayList<>();
        launchstates.add(LaunchState.Launched);

        if (start < 90000000000L) { // if request using second level timestamp
            start = start * 1000L;
        }
        if (orgInfo == null) {
            dashboard = playProxy.getPlayLaunchDashboard(MultiTenantContext.getCustomerSpace().toString(), null,
                    launchstates, start, 0L, 1000L, null, null, null, null, null);
        } else {
            Pair<String, String> effectiveOrgInfo = LookupIdMapUtils.getEffectiveOrgInfo(orgInfo);
            dashboard = playProxy.getPlayLaunchDashboard(MultiTenantContext.getCustomerSpace().toString(), null,
                    launchstates, start, 0L, 1000L, null, null, null, effectiveOrgInfo.getLeft(),
                    effectiveOrgInfo.getRight());
        }
        if (dashboard == null) {
            return filteredLaunchSummaries;
        }
        List<LaunchSummary> queriedSummaries = dashboard.getLaunchSummaries();

        Map<String, String> activePlays = new HashMap<String, String>();
        if (CollectionUtils.isNotEmpty(playIds)) {
            playIds.forEach(playId -> {
                if (!activePlays.containsKey(playId)) {
                    activePlays.put(playId, playId);
                }
            });
        }

        if (latest) {
            Map<String, String> match = new HashMap<String, String>();
            queriedSummaries.stream().forEach(launch -> {
                if (StringUtils.isNotBlank(launch.getLaunchId())) {
                    if (!match.containsKey(launch.getPlayName())) {
                        if (CollectionUtils.isNotEmpty(playIds)) {
                            if (activePlays.containsKey(launch.getPlayName())) {
                                match.put(launch.getPlayName(), launch.getLaunchId());
                                filteredLaunchSummaries.add(launch);
                            }
                        } else {
                            match.put(launch.getPlayName(), launch.getLaunchId());
                            filteredLaunchSummaries.add(launch);
                        }
                    }
                }
            });
        } else {
            queriedSummaries.stream().forEach(launch -> {
                if (StringUtils.isNotBlank(launch.getLaunchId())) {
                    if (CollectionUtils.isNotEmpty(playIds)) {
                        if (activePlays.containsKey(launch.getPlayName())) {
                            filteredLaunchSummaries.add(launch);
                        }
                    } else {
                        filteredLaunchSummaries.add(launch);
                    }
                }
            });
        }
        return filteredLaunchSummaries;
    }

    private List<Map<String, Object>> getAllProducts() {
        FrontEndQuery query = new FrontEndQuery();
        query.setAccountRestriction(null);
        query.setContactRestriction(null);
        query.addLookups(BusinessEntity.Product, InterfaceName.ProductId.name());
        query.addLookups(BusinessEntity.Product, InterfaceName.ProductName.name());
        query.setMainEntity(BusinessEntity.Product);

        return entityProxy.getProducts(MultiTenantContext.getCustomerSpace().toString()).getData();
    }

    private List<Map<String, Object>> handlePagination(long start, int offset, int maximum, List<Play> plays) {
        // init empty array for all products, we'll try to populate it only when
        // we need to (means some cross-sell model present)
        List<Map<String, Object>> allProducts = new ArrayList<>();
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
        playMap.put(PlaymakerConstants.PlayGroups, play.getPlayGroups().stream().map(PlayGroup::getDisplayName).collect(Collectors.toList()));
        if (play.getRatingEngine() != null) {
            RatingEngine ratingEngine = ratingEngineProxy
                    .getRatingEngine(MultiTenantContext.getCustomerSpace().toString(), play.getRatingEngine().getId());
            playMap.put(PlaymakerConstants.TargetProducts, getTargetProducts(ratingEngine, allProducts));
        }
        playMap.put(PlaymakerConstants.Workflow, PlayType.getIdForBIS(play.getPlayType().getDisplayName()));

        playMap.put(PlaymakerConstants.RowNum, rowNum);
        result.add(playMap);
        return playMap;
    }

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

        if (CollectionUtils.isNotEmpty(targetProductIds)) {
            if (CollectionUtils.isEmpty(allProducts)) {
                // initialize allProducts only once and only when it is needed
                log.info("Loading info about all products");
                allProducts.addAll(getAllProducts());
            }

            targetProductIds.forEach(productId -> {
                Map<String, Object> foundProduct = findProductById(productId, allProducts);
                if (foundProduct != null) {
                    HashMap<String, String> prod = new HashMap<>();
                    prod.put(PlaymakerConstants.DisplayName,
                            (String) foundProduct.get(InterfaceName.ProductName.name()));
                    prod.put(PlaymakerConstants.ExternalName,
                            (String) foundProduct.get(InterfaceName.ProductId.name()));
                    toReturn.add(prod);
                }
            });
        }
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
    public int getPlayCount(long start, List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo) {
        List<Play> plays = getPlayList(start, playgroupIds, syncDestination, orgInfo);
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
