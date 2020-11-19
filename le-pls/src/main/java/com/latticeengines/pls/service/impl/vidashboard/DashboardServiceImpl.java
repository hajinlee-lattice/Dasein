package com.latticeengines.pls.service.impl.vidashboard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.util.UuidUtil;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.vidashboard.DashboardService;
import com.latticeengines.pls.util.ElasticSearchUtil;

@Component("dashboardService")
public class DashboardServiceImpl implements DashboardService {

    private static final Logger log = LoggerFactory.getLogger(DashboardServiceImpl.class);

    private static final String INDEX_PATTERN_PREFIX = "index-pattern:";
    private static final String VISUALIZATION_PREFIX = "visualization:";
    private static final String DASHBOARD_PREFIX = "dashboard:";

    private static final String INDEX_PATTERN_NAME_PLACEHOLDER = "<INDEX_PATTERN_NAME>";
    private static final String INDEX_PATTERN_ID_PLACEHOLDER = "<INDEX_PATTERN_ID>";
    private static final String CREATE_TIME_PLACEHOLDER = "<CREATED_TIME>";
    private static final String PANEL_ID_PLACEHOLDER = "<PANEL%s_ID>";
    private static final String DASHBOARD_NAME_PLACEHOLDER = "<DASHBOARD_NAME>";
    private static final String NAME_PLACEHOLDER = "<NAME>";

    private static final String PATH_PREFIX = "com/latticeengines/pls/kibanaitems/%s";


    @Inject
    private RestHighLevelClient client;
    @Value("${cdl.elasticsearch.kibana.index}")
    private String kibanaIndex;

    private String indexPatternName = "";
    private String indexPatternId = "";
    //placeholder-> visualizationId
    private Map<String, String> visualizationMap = new HashMap<>();

    @Override
    public void create() {
        createIndexPattern();
        createVisualization();
        createDashboard();
    }

    private void createIndexPattern() {
        String json = "";
        try (InputStream inputStream =
                     getClass().getClassLoader().getResourceAsStream(String.format(PATH_PREFIX, "data_index_pattern" +
                             ".json"))) {
            json = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
            log.info("json is {}", json);
        } catch (IOException exception) {
            throw new LedpException(LedpCode.LEDP_00002, "Can't read data_index_pattern", exception);
        }
        log.info("file is {}", json);
        indexPatternName = "test_data_joy";
        indexPatternId = String.format("%s%s", INDEX_PATTERN_PREFIX, UuidUtil.getTimeBasedUuid());
        log.info("indexPatternId is {}.", indexPatternId);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        Date date = new Date(System.currentTimeMillis());
        json = json.replace(INDEX_PATTERN_NAME_PLACEHOLDER, indexPatternName).replace(CREATE_TIME_PLACEHOLDER,
                formatter.format(date));
        log.info("replaced json is {}.", json);
        ElasticSearchUtil.createDocument(client, kibanaIndex, indexPatternId, json);
    }

    private void createVisualization() {
        if (StringUtils.isEmpty(indexPatternName)) {
            return;
        }
        for (int i = 0; i < 5; i++) {
            String json = "";
            try (InputStream inputStream =
                         getClass().getClassLoader().getResourceAsStream(String.format(PATH_PREFIX, String.format(
                                 "employee_panel/panel_%s.json", i)))) {
                json = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
            } catch (IOException exception) {
                throw new LedpException(LedpCode.LEDP_00002, "Can't read panel file", exception);
            }
            String visualizationId = String.format("%s%s", VISUALIZATION_PREFIX, UuidUtil.getTimeBasedUuid());
            log.info("visualizationId is {}.", visualizationId);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            Date date = new Date(System.currentTimeMillis());
            json = json.replace(INDEX_PATTERN_NAME_PLACEHOLDER, indexPatternName).
                    replace(INDEX_PATTERN_ID_PLACEHOLDER, indexPatternId)
                    .replace(CREATE_TIME_PLACEHOLDER, formatter.format(date)).replace(NAME_PLACEHOLDER, "joyTest");
            ElasticSearchUtil.createDocument(client, kibanaIndex, visualizationId, json);
            visualizationMap.put(String.format(PANEL_ID_PLACEHOLDER, i), visualizationId);
        }
    }

    private void createDashboard() {
        if (MapUtils.isEmpty(visualizationMap)) {
            return;
        }
        String json = "";
        try (InputStream inputStream =
                     getClass().getClassLoader().getResourceAsStream(String.format(PATH_PREFIX, "employee_dashboard.json"))) {
            json = StreamUtils.copyToString(inputStream, Charset.defaultCharset());
        } catch (IOException exception) {
            throw new LedpException(LedpCode.LEDP_00002, "Can't read employee_dashboard", exception);
        }
        indexPatternName = "test_data_joy";
        indexPatternId = String.format("%s%s", DASHBOARD_PREFIX, UuidUtil.getTimeBasedUuid());
        log.info("indexPatternId is {}.", indexPatternId);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        Date date = new Date(System.currentTimeMillis());
        json = json.replace(INDEX_PATTERN_NAME_PLACEHOLDER, indexPatternName).replace(CREATE_TIME_PLACEHOLDER,
                formatter.format(date)).replace(DASHBOARD_NAME_PLACEHOLDER, "joyTest_Employee");
        ElasticSearchUtil.createDocument(client, kibanaIndex, indexPatternId, json);
    }

}
