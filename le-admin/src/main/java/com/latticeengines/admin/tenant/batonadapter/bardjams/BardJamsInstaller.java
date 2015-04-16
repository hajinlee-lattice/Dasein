package com.latticeengines.admin.tenant.batonadapter.bardjams;

import java.io.FileReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.common.base.Function;
import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.BardJamsRequestStatus;
import com.latticeengines.domain.exposed.admin.BardJamsTenants;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class BardJamsInstaller implements CustomerSpaceServiceInstaller {

    private int timeout = 30000;

    private final Log log = LogFactory.getLog(this.getClass());

    private BardJamsEntityMgr bardJamsEntityMgr;

    @Override
    public DocumentDirectory install(CustomerSpace space, String serviceName, int dataVersion,
            Map<String, String> properties) {

        BardJamsTenants tenant = pupulateRequest(space, serviceName, dataVersion, properties);
        bardJamsEntityMgr.create(tenant);

        log.info("Created BardJams Request=" + tenant.toString());

        boolean isSuccessful = checkRequest(tenant);
        if (isSuccessful) {
            log.info("Successfully created BardJams Request=" + tenant);
        } else {
            log.info("Failed to create BardJams Request=" + tenant);
            throw new LedpException(LedpCode.LEDP_18027);
        }

        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), space.getContractId(),
                space.getTenantId(), space.getSpaceId(), serviceName);
        docPath.append("Properties");

        DocumentDirectory docDirectory = new DocumentDirectory(docPath, new ConfigGetChildrenFunction(docPath,
                properties));
        return docDirectory;
    }

    protected void setBardJamsEntityMgr(BardJamsEntityMgr bardJamsEntityMgr) {
        this.bardJamsEntityMgr = bardJamsEntityMgr;
    }

    protected void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    private boolean checkRequest(BardJamsTenants request) {
        long currTime = System.currentTimeMillis();
        long endTime = currTime + timeout;
        boolean isSuccessful = false;
        while (currTime < endTime) {
            log.info("Starting to check status of request=" + request.toString());
            BardJamsTenants newRequest = bardJamsEntityMgr.findByKey(request);
            if (newRequest.getStatus().equals(BardJamsRequestStatus.FINISHED.toString())) {
                isSuccessful = true;
                break;
            }
            if (newRequest.getStatus().equals(BardJamsRequestStatus.FAILED.toString())) {
                isSuccessful = false;
                break;
            }
            try {
                long wait_interval_mills = 3000L;
                Thread.sleep(wait_interval_mills);
            } catch (Exception ex) {
                log.warn("Warning!", ex);
            }
            currTime = System.currentTimeMillis();
        }

        return isSuccessful;
    }

    private BardJamsTenants pupulateRequest(CustomerSpace space, String serviceName, int dataVersion,
            Map<String, String> properties) {
        BardJamsTenants request = new BardJamsTenants();

        request.setTenant(properties.get("Tenant"));
        request.setTenantType(properties.get("TenantType"));
        request.setDlTenantName(properties.get("DL_TenantName"));
        request.setDlUrl(properties.get("DL_URL"));
        request.setDlUser(properties.get("DL_User"));
        request.setDlPassword(properties.get("DL_Password"));
        request.setNotificationEmail(properties.get("NotificationEmail"));
        request.setNotifyEmailJob(properties.get("NotifyEmailJob"));
        request.setJamsUser(properties.get("JAMSUser"));
        request.setImmediateFolderStruct(properties.get("ImmediateFolderStruct"));
        request.setScheduledFolderStruct(properties.get("ScheduledFolderStruct"));
        request.setDanteManifestPath(properties.get("DanteManifestPath"));
        request.setQueueName(properties.get("Queue_Name"));
        request.setAgentName(properties.get("Agent_Name"));
        request.setWeekdayScheduleName(properties.get("WeekdaySchedule_Name"));
        request.setWeekendScheduleName(properties.get("WeekendSchedule_Name"));
        request.setDataLaunchPath(properties.get("Data_LaunchPath"));
        request.setDataArchivePath(properties.get("Data_ArchivePath"));
        request.setDataLoaderToolsPath(properties.get("DataLoaderTools_Path"));
        request.setDanteToolPath(properties.get("DanteTool_Path"));
        String active = properties.get("Active");
        if (active != null) {
            request.setActive(Integer.parseInt(active));
        }
        request.setDanteQueueName(properties.get("Dante_Queue_Name"));
        request.setLoadGroupList(properties.get("LoadGroupList"));
        request.setStatus(BardJamsRequestStatus.NEW.getStatus());

        return request;
    }

    @Override
    public DocumentDirectory getDefaultConfiguration(String serviceName) {
        Path path = PathBuilder.buildServicePath(CamilleEnvironment.getPodId(), serviceName);
        path.append("Properties");
        DocumentDirectory docDirectory = null;
        try {
            String jsonFile = Thread.currentThread().getContextClassLoader().getResource("bardjams.json").getPath();
            FileReader reader = new FileReader(jsonFile);
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(reader);
            docDirectory = new DocumentDirectory(path, new DefaultConfigGetChildrenFunction(path, jsonObject));
            return docDirectory;

        } catch (Exception ex) {
            log.error("Failed to load json file!", ex);
            throw new LedpException(LedpCode.LEDP_18028);
        }
    }

    private class DefaultConfigGetChildrenFunction implements Function<Path, List<Map.Entry<Document, Path>>> {

        private JSONObject jsonObject;
        private Path rootPath;

        public DefaultConfigGetChildrenFunction(Path path, JSONObject jsonObject) {
            this.rootPath = path;
            this.jsonObject = jsonObject;
        }

        @Override
        public List<Entry<Document, Path>> apply(Path parentPath) {

            List<Map.Entry<Document, Path>> result = new ArrayList<Map.Entry<Document, Path>>();
            if (rootPath.getParts().size() != parentPath.getParts().size()) {
                return result;
            }
            JSONArray jsonArray = (JSONArray) jsonObject.get("BardJams");
            if (jsonArray != null && jsonArray.size() > 0) {
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = (JSONObject) jsonArray.get(i);
                    String childPath = (String) jsonObject.get("id");
                    String data = (String) jsonObject.get("defaultValue");
                    Document doc = new Document(data);
                    result.add(new AbstractMap.SimpleEntry<Document, Path>(doc, parentPath.append(childPath)));
                }
            }
            return result;
        }
    }

    private class ConfigGetChildrenFunction implements Function<Path, List<Map.Entry<Document, Path>>> {
        private Path rootPath;
        private Map<String, String> properties;

        public ConfigGetChildrenFunction(Path path, Map<String, String> properties) {
            this.rootPath = path;
            this.properties = properties;
        }

        @Override
        public List<Entry<Document, Path>> apply(Path parentPath) {

            List<Map.Entry<Document, Path>> result = new ArrayList<Map.Entry<Document, Path>>();
            if (rootPath.getParts().size() != parentPath.getParts().size() || properties == null
                    || properties.size() == 0) {
                return result;
            }
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String childPath = entry.getKey();
                String data = entry.getValue();
                Document doc = new Document(data);
                result.add(new AbstractMap.SimpleEntry<Document, Path>(doc, parentPath.append(childPath)));
            }
            return result;
        }
    }
}
