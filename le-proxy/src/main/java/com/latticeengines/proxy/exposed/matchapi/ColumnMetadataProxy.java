package com.latticeengines.proxy.exposed.matchapi;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatchers.AMApiUpdate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.network.exposed.propdata.ColumnMetadataInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("columnMetadataProxyMatchapi")
public class ColumnMetadataProxy extends BaseRestApiProxy implements ColumnMetadataInterface {

    private static final String AM_REPO = "AMCollection";

    private WatcherCache<String, List<ColumnMetadata>> enrichmentColumnsCache;
    private WatcherCache<String, DataCloudVersion> latestDataCloudVersionCache;
    private WatcherCache<String, AttributeRepository> amAttrRepoCache;
    private boolean scheduled = false;

    @Autowired
    private BatonServiceImpl batonService;

    @SuppressWarnings("unchecked")
    public ColumnMetadataProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/metadata");
    }

    @SuppressWarnings("unchecked")
    @PostConstruct
    private void postConstruct() {
        enrichmentColumnsCache = WatcherCache.builder(AMApiUpdate.name()) //
                .name("EnrichmentColumnsCache") //
                .maximum(20) //
                .load(dataCloudVersion -> requestColumnSelection(Predefined.Enrichment, (String) dataCloudVersion)) //
                .initKeys(new String[] { "" }) //
                .build();
        latestDataCloudVersionCache = WatcherCache.builder(AMApiUpdate.name()) //
                .name("LatestDataCloudVersionCache") //
                .maximum(20) //
                .load(compatibleVersion -> requestLatestVersion((String) compatibleVersion)) //
                .initKeys(new String[] { "" }) //
                .build();
        amAttrRepoCache = WatcherCache.builder(AMApiUpdate.name()) //
                .name("AMAttrRepoCache") //
                .maximum(1) //
                .load(key -> getAttrRepoViaREST()) //
                .initKeys(new String[] { AM_REPO }) //
                .build();
    }

    public void scheduleDelayedInitOfEnrichmentColCache() {
        synchronized (this) {
            if (!scheduled) {
                enrichmentColumnsCache.scheduleInit(10, TimeUnit.MINUTES);
                scheduled = true;
            }
        }
    }

    @Override
    public List<ColumnMetadata> columnSelection(Predefined selectName, String dataCloudVersion) {
        if (Predefined.Enrichment.equals(selectName)) {
            try {
                if (StringUtils.isEmpty(dataCloudVersion)) {
                    dataCloudVersion = "";
                }
                return enrichmentColumnsCache.get(dataCloudVersion);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get enrichment column metadata from loading cache.", e);
            }
        } else {
            return requestColumnSelection(selectName, dataCloudVersion);
        }
    }

    @Override
    public DataCloudVersion latestVersion(String compatibleVersion) {
        try {
            if (StringUtils.isEmpty(compatibleVersion)) {
                compatibleVersion = "";
            }
            return latestDataCloudVersionCache.get(compatibleVersion);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get latest version for dataCloudVersion " //
                    + compatibleVersion + " from loading cache.", e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private List<ColumnMetadata> requestColumnSelection(Predefined selectName, String dataCloudVersion) {
        String url = constructUrl("/predefined/{selectName}", String.valueOf(selectName.name()));
        if (StringUtils.isNotBlank(dataCloudVersion)) {
            url = constructUrl("/predefined/{selectName}?datacloudversion={dataCloudVersion}",
                    String.valueOf(selectName.name()), dataCloudVersion);
        }
        List<Map<String, Object>> metadataObjs = get("columnSelection", url, List.class);
        List<ColumnMetadata> metadataList = new ArrayList<>();
        if (metadataObjs == null) {
            return metadataList;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            for (Map<String, Object> obj : metadataObjs) {
                ColumnMetadata metadata = mapper.treeToValue(mapper.valueToTree(obj), ColumnMetadata.class);
                metadataList.add(metadata);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadataList;
    }

    @SuppressWarnings({ "unchecked" })
    public AttributeRepository getAttrRepo() {
        return amAttrRepoCache.get(AM_REPO);
    }

    private AttributeRepository getAttrRepoViaREST() {
        String url = constructUrl("/attrrepo");
        return get("get AM attr repo", url, AttributeRepository.class);
    }

    private DataCloudVersion requestLatestVersion(String compatibleVersion) {
        String url;
        if (StringUtils.isNotBlank(compatibleVersion)) {
            url = constructUrl("/versions/latest?compatibleto={compatibleVersion}", compatibleVersion);
        } else {
            url = constructUrl("/versions/latest");
        }
        DataCloudVersion latestVersion = get("latest version", url, DataCloudVersion.class);
        return latestVersion;
    }

}
