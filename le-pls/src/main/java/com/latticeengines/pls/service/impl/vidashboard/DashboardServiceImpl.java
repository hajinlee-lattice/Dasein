package com.latticeengines.pls.service.impl.vidashboard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.UuidUtil;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilterValue;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.vidashboard.DashboardService;
import com.latticeengines.pls.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.cdl.DashboardProxy;

@Component("dashboardService")
public class DashboardServiceImpl implements DashboardService {

    private static final Logger log = LoggerFactory.getLogger(DashboardServiceImpl.class);

    private static final String INDEX_PATTERN_PREFIX = "index-pattern:";
    private static final String VISUALIZATION_PREFIX = "visualization:";
    private static final String DASHBOARD_PREFIX = "dashboard:";

    private static final String DASHBOARD_JSONFILE_SUFFIX = "%s_dashboard.json";
    private static final String PANEL_DIR_PATH = "%s_panel/";

    private static final String INDEX_PATTERN_NAME_PLACEHOLDER = "<INDEX_PATTERN_NAME>";
    private static final String INDEX_PATTERN_ID_PLACEHOLDER = "<INDEX_PATTERN_ID>";
    private static final String CREATE_TIME_PLACEHOLDER = "<CREATED_TIME>";
    private static final String PANEL_ID_PLACEHOLDER = "<PANEL%s_ID>";
    private static final String NAME_PLACEHOLDER = "<NAME>";
    private static final String COMPANY_TABLE_PLACEHOLDER = "<PANEL_COMPANY_TABLE_ID>";
    private static final String COMPANY_LABEL_PLACEHOLDER = "<PANEL_COMPANY_LABEL_ID>";
    private static final String PAGE_GROUP_PLACEHOLDER = "<PANEL_PAGE_GROUP_ID>";
    private static final String KIBANA_URL_PLACEHOLDER = "<KIBANA_URL>";
    private static final String DASHBOARD_URL_OTHER_FILTER_PLACEHOLDER = "<OTHER_FILTER>";
    private static final String DASHBOARD_URL_ID_PLACEHOLDER = "<DASHBOARD_ID>";
    private static final String DASHBOARD_NAME_PLACEHOLDER = "<DASHBOARD_NAME>";

    private static final String PATH_PREFIX = "com/latticeengines/pls/kibanaitems/%s";

    private static final int MAX_RETRY = 3;

    @Inject
    private RestHighLevelClient client;
    @Inject
    private DashboardProxy dashboardProxy;
    @Value("${cdl.elasticsearch.kibana.index}")
    private String kibanaIndex;
    @Value("${cdl.elasticsearch.kibana.url}")
    private String kibanaUrl;
    //indexPatternName need using esIndexName or esIndexNamePrefix(Regular expression)
    private String indexPatternName;
    private String indexPatternId;
    private String jsonFile;

    private List<String> dashboardNameList = Arrays.asList("employee", "industry", "location", "overview", "page",
            "page_group", "revenue");
    //placeholder-> visualizationId : COMPANY_TABLE_PLACEHOLDER && COMPANY_LABEL_PLACEHOLDER
    private Map<String, String> companyVisualizationMap;
    //dashboardName-><placeholder, visualizationId>
    private Map<String, Map<String, String>> dashboardVisualizationMap;
    //dashboardName->dashboardId
    private Map<String, String> dashboardIdMap;
    //dashboard List Name -> dashboard Real Name
    private Map<String, String> dashboardRealNameMap;

    @Override
    public void create(String customerSpace, String esIndexName) {
        createIndexPattern(esIndexName);
        String namePrefix = generateNameprefix(customerSpace);
        createVisualization(namePrefix);
        if (MapUtils.isEmpty(companyVisualizationMap)) {
            log.error("visualization created failed, can't create dashboard.");
            return;
        }
        createDashboard(namePrefix);
        createDashboardUrl(customerSpace);
    }

    @Override
    public DashboardResponse getDashboardList(String customerSpace) {
        DashboardResponse res = new DashboardResponse();
        res.setDashboardUrls(getDashboardMap(customerSpace));
        res.setFilters(getFilterMap(customerSpace));
        return res;
    }

    private Map<String, String> getDashboardMap(String customerSpace) {
        List<Dashboard> dashboardList = dashboardProxy.getDashboards(customerSpace);
        Map<String, String> dashboardMap = new HashMap<>();
        if (CollectionUtils.isEmpty(dashboardList)) {
            log.warn("dashboard list is empty for tenant {}.", customerSpace);
            return dashboardMap;
        }
        for (Dashboard dashboard : dashboardList) {
            dashboardMap.put(dashboard.getName(), dashboard.getDashboardUrl());
        }
        return dashboardMap;
    }

    private Map<String, List<DashboardFilterValue>> getFilterMap(String customerSpace) {
        List<DashboardFilter> filters = dashboardProxy.getDashboardFilters(customerSpace);
        Map<String, List<DashboardFilterValue>> filterMap = new HashMap<>();
        if (MapUtils.isEmpty(filterMap)) {
            log.warn("dashboard filter is empty, tenant is {}.", customerSpace);
            return filterMap;
        }
        for (DashboardFilter filter : filters) {
            filterMap.put(filter.getName(), filter.getFilterValue());
        }
        return filterMap;
    }

    private void createIndexPattern(String esIndexName) {
        jsonFile = "";
        try (InputStream inputStream =
                     getClass().getClassLoader().getResourceAsStream(String.format(PATH_PREFIX, "data_index_pattern" +
                             ".json"))) {
            jsonFile = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
        } catch (IOException exception) {
            throw new LedpException(LedpCode.LEDP_00002, "Can't read data_index_pattern", exception);
        }
        log.info("indexPatternJson is {}, indexName is {}.", jsonFile, esIndexName);
        indexPatternName = esIndexName;
        indexPatternId = UuidUtil.getTimeBasedUuid().toString();
        log.info("indexPatternId is {}, indexName is {}.", indexPatternId, esIndexName);

        jsonFile = jsonFile.replace(INDEX_PATTERN_NAME_PLACEHOLDER, indexPatternName).replace(CREATE_TIME_PLACEHOLDER,
                getDate());
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {

                ElasticSearchUtils.createDocument(client, kibanaIndex, String.format("%s%s", INDEX_PATTERN_PREFIX,
                        indexPatternId), jsonFile);
                return 0;
            });
        } catch (IOException e) {
            log.error(String.format("Failed to create index-pattern %s", String.format("%s%s", INDEX_PATTERN_PREFIX,
                    indexPatternId)), e);
        }
    }



    private void createVisualization(String namePrefix) {
        companyVisualizationMap = new HashMap<>();
        dashboardVisualizationMap = new HashMap<>();
        //visualization doc id->JsonString
        Map<String, String> visualizationJsonFileMap = new HashMap<>();
        String companyLabelFilePath = String.format(PATH_PREFIX, "panel_company_label.json");
        companyVisualizationMap.put(COMPANY_LABEL_PLACEHOLDER, createVisualization(namePrefix,
                companyLabelFilePath, visualizationJsonFileMap));
        String companyTableFilePath = String.format(PATH_PREFIX, "panel_company_table.json");
        companyVisualizationMap.put(COMPANY_TABLE_PLACEHOLDER, createVisualization(namePrefix, companyTableFilePath,
                visualizationJsonFileMap));
        String pageGroupFilePath = String.format(PATH_PREFIX, "panel_page_group.json");
        companyVisualizationMap.put(PAGE_GROUP_PLACEHOLDER, createVisualization(namePrefix, pageGroupFilePath, visualizationJsonFileMap));
        for (String dashboardName : dashboardNameList) {
            Map<String, String> visualizationMap = new HashMap<>();
            String panelPath = String.format(PATH_PREFIX, String.format(PANEL_DIR_PATH, dashboardName));
            log.info("panel_path is {}", panelPath);
            String fileJson;
            try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(panelPath)) {
                fileJson = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
                log.info("panel List is {}", fileJson);
                String[] files = fileJson.split("\n");
                for (String file : files) {
                    String filePath = String.format("%s%s", panelPath, file);
                    String number = Pattern.compile("[^0-9]").matcher(file).replaceAll("");
                    log.info("panel id is {}", number);
                    visualizationMap.put(String.format(PANEL_ID_PLACEHOLDER, number), createVisualization(namePrefix,
                            filePath, visualizationJsonFileMap));
                }
                dashboardVisualizationMap.put(dashboardName, visualizationMap);
            } catch (IOException exception) {
                throw new LedpException(LedpCode.LEDP_00002, "Can't read visualization", exception);
            }
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createDocuments(client, kibanaIndex, visualizationJsonFileMap);
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create visualization documents, namePrefix is %s.", namePrefix), e);
            throw new IllegalStateException(String.format("Failed to create visualization documents, namePrefix is %s.", namePrefix));
        }
    }

    private void createDashboard(String namePrefix) {
        dashboardIdMap = new HashMap<>();
        dashboardRealNameMap = new HashMap<>();
        //dashboard doc id -> dashboard json file.
        Map<String, String> dashboardFileMap = new HashMap<>();
        for (String dashboardName : dashboardNameList) {
            dashboardIdMap.put(dashboardName, createDashboard(namePrefix, dashboardName, dashboardFileMap));
        }
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_RETRY);
        try {retry.execute(context -> {
            ElasticSearchUtils.createDocuments(client, kibanaIndex, dashboardFileMap);
            return 0;
        });
        } catch (IOException e) {
            log.error(String.format("Failed to create dashboards, namePrefix is %s.", namePrefix), e);
            throw new IllegalStateException(String.format("Failed to create dashboards, namePrefix is %s.",
                    namePrefix));
        }
    }

    private String createVisualization(String namePrefix, String filePath, Map<String, String> visualizationFiles) {
        if (StringUtils.isEmpty(indexPatternName) || StringUtils.isEmpty(filePath)) {
            return "";
        }
        log.info("visualization file path is {}.", filePath);
        jsonFile = "";
        try (InputStream inputStream =
                     getClass().getClassLoader().getResourceAsStream(filePath)) {
            jsonFile = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
        } catch (IOException exception) {
            throw new LedpException(LedpCode.LEDP_00002, "Can't read panel file", exception);
        }
        String visualizationId = UuidUtil.getTimeBasedUuid().toString();
        log.info("visualizationId is {}.", visualizationId);
        jsonFile = jsonFile.replace(INDEX_PATTERN_NAME_PLACEHOLDER, indexPatternName).
                replace(INDEX_PATTERN_ID_PLACEHOLDER, indexPatternId)
                .replace(CREATE_TIME_PLACEHOLDER, getDate()).replace(NAME_PLACEHOLDER, namePrefix);
        log.info("visualization details is {}", jsonFile);
        visualizationFiles.put(String.format("%s%s", VISUALIZATION_PREFIX, visualizationId), jsonFile);
        return visualizationId;
    }

    private String createDashboard(String namePrefix, String dashboardName, Map<String, String> dashboardFileMap) {
        String dashboardFilePath = String.format(PATH_PREFIX, String.format(DASHBOARD_JSONFILE_SUFFIX, dashboardName));
        Map<String, String> dashboardRelatedVisualizationMap = dashboardVisualizationMap.get(dashboardName);
        if (MapUtils.isEmpty(dashboardRelatedVisualizationMap)) {
            return "";
        }
        jsonFile = "";
        try (InputStream inputStream =
                     getClass().getClassLoader().getResourceAsStream(dashboardFilePath)) {
            jsonFile = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
        } catch (IOException exception) {
            throw new LedpException(LedpCode.LEDP_00002, "Can't read dashboard", exception);
        }
        String dashboardRealName = String.format("%s_%s", namePrefix, dashboardName);
        String dashboardId = UuidUtil.getTimeBasedUuid().toString();
        log.info("dashboardId is {}.", dashboardId);
        jsonFile = jsonFile.replace(INDEX_PATTERN_NAME_PLACEHOLDER, indexPatternName).replace(CREATE_TIME_PLACEHOLDER,
                getDate()).replace(DASHBOARD_NAME_PLACEHOLDER, dashboardRealName)
                .replace(COMPANY_LABEL_PLACEHOLDER, companyVisualizationMap.get(COMPANY_LABEL_PLACEHOLDER))
                .replace(COMPANY_TABLE_PLACEHOLDER, companyVisualizationMap.get(COMPANY_TABLE_PLACEHOLDER))
                .replace(PAGE_GROUP_PLACEHOLDER, companyVisualizationMap.get(PAGE_GROUP_PLACEHOLDER));
        for (Map.Entry<String, String> entry : dashboardRelatedVisualizationMap.entrySet()) {
            jsonFile = jsonFile.replace(entry.getKey(), entry.getValue());
        }
        log.info("Dashboard is {}", jsonFile);
        dashboardFileMap.put(String.format("%s%s", DASHBOARD_PREFIX, dashboardId), jsonFile);
        dashboardRealNameMap.put(dashboardName, dashboardRealName);
        return dashboardId;
    }

    private void createDashboardUrl(String customerSpace) {
        String urlFilePath = String.format(PATH_PREFIX, "dashboard_url.json");
        Map<String, String> urlMap = getMapFromJson(urlFilePath);
        if (MapUtils.isEmpty(urlMap)) {
            log.error("Can't get url templates, create dashboard url failed.");
            return;
        }
        String urlFilterFilePath = String.format(PATH_PREFIX, "dashboard_url_filter.json");
        Map<String, String> urlFilterMap = getMapFromJson(urlFilterFilePath);
        if (MapUtils.isEmpty(urlFilterMap)) {
            log.error("Can't get url filter, create dashboard url failed.");
        }
        List<Dashboard> dashboards = new ArrayList<>();
        for (String dashboardName : urlMap.keySet()) {
            String dashboardUrlTemplate = urlMap.get(dashboardName);
            String dashboardId = dashboardIdMap.get(dashboardName);
            if (StringUtils.isEmpty(dashboardId)) {
                log.error("can't create DashboardUrl for dashboard {}, because can't find this dashboard id.", dashboardName);
                continue;
            }
            String dashboardUrlFilter = urlFilterMap.get(dashboardName);
            String dashboardUrl =
                    dashboardUrlTemplate.replace(KIBANA_URL_PLACEHOLDER, kibanaUrl)
                            .replace(DASHBOARD_URL_ID_PLACEHOLDER, dashboardId)
                            .replace(INDEX_PATTERN_NAME_PLACEHOLDER, indexPatternName);
            if (StringUtils.isNotEmpty(dashboardUrlFilter)) {
                dashboardUrl =
                        dashboardUrl.replace(DASHBOARD_URL_OTHER_FILTER_PLACEHOLDER, dashboardUrlFilter)
                                .replace(INDEX_PATTERN_ID_PLACEHOLDER, indexPatternId);
            }
            Dashboard dashboard = new Dashboard();
            dashboard.setName(dashboardRealNameMap.get(dashboardName));
            dashboard.setDashboardUrl(dashboardUrl);
            dashboards.add(dashboard);
        }
        log.info("dashboards is {}", JsonUtils.serialize(dashboards));
        dashboardProxy.createDashboardList(customerSpace, dashboards);

    }

    private Map<String, String> getMapFromJson(String filePath) {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath)) {
            String json = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
            Map<?, ?> map = JsonUtils.deserialize(json, Map.class);
            Map<String, String> convertMaps = JsonUtils.convertMap(map, String.class, String.class);
            log.info("Map size is {}.", convertMaps.size());
            return convertMaps;
        } catch (IOException exception) {
            throw new LedpException(LedpCode.LEDP_00002, "Can't read jsonFile from map.", exception);
        }
    }

    private String getDate() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date date = new Date(System.currentTimeMillis());
        return formatter.format(date);
    }

    private String generateNameprefix(String customerSpace) {
        String uuid = RandomStringUtils.randomAlphabetic(6);
        return String.format("%s_%s", customerSpace, uuid);
    }

}
