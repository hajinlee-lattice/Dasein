package com.latticeengines.playmaker.dao.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;

public class PlaymakerRecommendationDaoImpl extends BaseGenericDaoImpl implements PlaymakerRecommendationDao {

    public PlaymakerRecommendationDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<Integer> playIds) {
        String sql = "SELECT * FROM (SELECT L.[PreLead_ID] AS ID, L.Account_ID AS AccountID, L.[LaunchRun_ID] AS LaunchID, "
                + "PL.[Display_Name] AS DisplayName, A.Display_Name AS CompanyName, "
                + "COALESCE(PL.[Description], L.[Description]) AS Description, "
                + "CASE WHEN A.CRMAccount_External_ID IS NOT NULL THEN A.CRMAccount_External_ID ELSE A.Alt_ID END AS SfdcAccountID, "
                + "L.[Play_ID] AS PlayID, DATEDIFF(s,'19700101 00:00:00:000', R.Start) AS LaunchDate, "
                + getLikelihood()
                + "C.Value AS PriorityDisplayName, P.Priority_ID AS PriorityID, "
                + "CASE WHEN L.[Expiration_Date] > '2030-03-15' THEN 1899763200 ELSE DATEDIFF(s,'19700101 00:00:00:000', L.[Expiration_Date]) END AS ExpirationDate, "
                + getMonetaryValue()
                + "M.ISO4217_ID AS MonetaryValueIso4217ID, "
                + "(SELECT TOP 1  ISNULL(T.[Display_Name], '') + '|' + ISNULL(T.[Phone_Number], '') + '|' + ISNULL(T.[Email_Address], '') "
                + " + '|' +  ISNULL(T.[Address_Street_1], '') + '|' + ISNULL(T.[City], '') + '|' + ISNULL(T.[State_Province], '') "
                + " + '|' + ISNULL(T.[Country], '') + '|' + ISNULL(T.[Zip], '') "
                + getSfdcContactID()
                + "FROM [LEContact] T WHERE T.Account_ID = A.LEAccount_ID) AS Contacts, "
                + "DATEDIFF(s,'19700101 00:00:00:000', L.[Last_Modification_Date]) AS LastModificationDate, "
                + "ROW_NUMBER() OVER ( ORDER BY L.[Last_Modification_Date], L.[PreLead_ID]) RowNum "
                + getRecommendationFromWhereClause(syncDestination, playIds)
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);
        source.addValue("syncDestination", syncDestination);
        if (!CollectionUtils.isEmpty(playIds)) {
            source.addValue("playIds", playIds);
        }

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertContacts(results);

        return results;
    }

    protected String getSfdcContactID() {
        return " + '|' + ISNULL(T.[External_id], '') ";
    }

    protected String getMonetaryValue() {
        return "L.[Likelihood] / 100 * PL.[Avg_Revenue_Per_Account] AS MonetaryValue, ";
    }

    protected String getLikelihood() {
        return "CASE WHEN L.[Likelihood] > 0 AND L.[Likelihood] < 2 THEN 1 ELSE FLOOR(L.[Likelihood]) END AS Likelihood, ";
    }

    protected void convertContacts(List<Map<String, Object>> results) {
        if (CollectionUtils.isNotEmpty(results)) {
            for (Map<String, Object> record : results) {
                String contacts = (String) record.get("Contacts");
                if (contacts != null) {
                    String[] contactArray = contacts.split("[|]", -1);
                    if (contactArray.length >= 9) {
                        // On recommendation record as well.
                        record.put("SfdcContactID", contactArray[8]);
                        
                        List<Map<String, Object>> contactList = new ArrayList<>(1);
                        Map<String, Object> contactMap = new HashMap<>();
                        contactMap.put("Name", contactArray[0]);
                        contactMap.put("Phone", contactArray[1]);
                        contactMap.put("Email", contactArray[2]);
                        contactMap.put("Address", contactArray[3]);
                        contactMap.put("City", contactArray[4]);
                        contactMap.put("State", contactArray[5]);
                        contactMap.put("Country", contactArray[6]);
                        contactMap.put("ZipCode", contactArray[7]);
                        contactMap.put("SfdcContactID", contactArray[8]);
                        contactList.add(contactMap);
                        record.put("Contacts", contactList);
                        
                    }

                }
            }
        }
    }

    @Override
    public int getRecommendationCount(long start, int syncDestination, List<Integer> playIds) {
        String sql = "SELECT COUNT(*) " + getRecommendationFromWhereClause(syncDestination, playIds);

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("syncDestination", syncDestination);
        if (!CollectionUtils.isEmpty(playIds)) {
            source.addValue("playIds", playIds);
        }
        return queryForObject(sql, source, Integer.class);
    }

    protected String getRecommendationFromWhereClause(int syncDestination, List<Integer> playIds) {
        String whereClause = "FROM [PreLead] L WITH (NOLOCK) LEFT OUTER JOIN LaunchRun R WITH (NOLOCK) "
                + "ON L.[LaunchRun_ID] = R.[LaunchRun_ID]  AND R.Launch_Stage = 0 JOIN LEAccount A WITH (NOLOCK) "
                + "ON L.Account_ID = A.LEAccount_ID AND A.IsActive = 1 JOIN Play PL "
                + "ON L.Play_ID = PL.Play_ID AND PL.IsActive = 1 AND PL.IsVisible = 1 JOIN Priority P WITH (NOLOCK) "
                + "ON L.Priority_ID = P.Priority_ID JOIN ConfigResource C WITH (NOLOCK) "
                + "ON P.Display_Text_Key = C.Key_Name AND C.Locale_ID = -1 JOIN Currency M WITH (NOLOCK) "
                + "ON L.[Monetary_Value_Currency_ID] = M.Currency_ID "
                + "WHERE L.Status = 2800 AND L.IsActive = 1 AND " + "L.Synchronization_Destination in ("
                + getDestinationonValues(syncDestination) + ") " + "%s "
                + "AND DATEDIFF(s,'19700101 00:00:00:000',L.[Last_Modification_Date]) >= :start ";

        StringBuilder extraFilter = new StringBuilder();
        if (CollectionUtils.isEmpty(playIds)) {
            extraFilter.append("");
        } else {
            extraFilter.append("AND L.Play_ID IN (:playIds) ");
        }
        return String.format(whereClause, extraFilter.toString());
    }

    private String getDestinationonValues(int syncDestination) {
        switch (syncDestination) {
        case 0:
            return "0";
        case 1:
            return "1";
        case 2:
            return "0,1";
        default:
            return "0";
        }

        // switch (syncDestination) {
        // case 0:
        // return "0,2";
        // case 1:
        // return "1,2";
        // case 2:
        // return "0,1,2";
        // default:
        // return "0,2";
        // }
    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds) {
        String sql = "SELECT * FROM (SELECT PL.[Play_ID] AS ID, PL.[External_ID] AS ExternalID, PL.[Display_Name] AS DisplayName, "
                + "PL.[Description] AS Description, PL.[Average_Probability] AS AverageProbability,"
                + "DATEDIFF(s,'19700101 00:00:00:000', PL.[Last_Modification_Date]) AS LastModificationDate, "
                + "(SELECT DISTINCT G.Display_Name + '|' as [text()] FROM PlayGroupMap M JOIN PlayGroup G "
                + "ON M.PlayGroup_ID = G.PlayGroup_ID WHERE M.Play_ID = PL.Play_ID FOR XML PATH ('')) AS PlayGroups, "
                + "(SELECT DISTINCT P.Display_Name + '|' + P.[External_Name] + '|' as [text()] "
                + "FROM [ProductGroupMap] M JOIN Product P ON M.Product_ID = P.Product_ID "
                + "WHERE M.[ProductGroup_ID] = PL.[Target_ProductGroup_ID] FOR XML PATH ('')) AS TargetProducts, "
                + "(SELECT W.[External_ID] FROM [PlayWorkflowType] W WHERE PL.[PlayWorkflowType_ID] = W.[PlayWorkflowType_ID]) AS Workflow, "
                + "ROW_NUMBER() OVER ( ORDER BY PL.[Last_Modification_Date], PL.[Play_ID] ) RowNum "
                + getPlayFromWhereClause(playgroupIds)
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);
        if (!CollectionUtils.isEmpty(playgroupIds)) {
            source.addValue("playgroupIds", playgroupIds);
        }

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertToList("PlayGroups", results);
        convertToMapList("TargetProducts", results);

        return results;
    }

    private void convertToMapList(String key, List<Map<String, Object>> results) {
        if (CollectionUtils.isEmpty(results)) {
            return;
        }
        for (Map<String, Object> record : results) {
            String value = (String) record.get(key);
            if (value != null) {
                String[] valueArray = StringUtils.split(value, "|");
                int len = valueArray.length / 2 * 2;
                List<Map<String, Object>> valueList = new ArrayList<>();
                for (int i = 0; i < len - 1; i += 2) {
                    Map<String, Object> valueMap = new HashMap<>();
                    valueMap.put("DisplayName", valueArray[i]);
                    valueMap.put("ExternalName", valueArray[i + 1]);
                    valueList.add(valueMap);
                }
                record.put(key, valueList);
            }
        }
    }

    private void convertToList(String key, List<Map<String, Object>> results) {
        if (CollectionUtils.isEmpty(results)) {
            return;
        }
        for (Map<String, Object> record : results) {
            String value = (String) record.get(key);
            if (value != null) {
                String[] valueArray = StringUtils.split(value, "|");
                record.put(key, Arrays.asList(valueArray));
            }
        }
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        String sql = "SELECT COUNT(*) " + getPlayFromWhereClause(playgroupIds);
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(playgroupIds)) {
            source.addValue("playgroupIds", playgroupIds);
        }
        return queryForObject(sql, source, Integer.class);
    }

    private String getPlayFromWhereClause(List<Integer> playgroupIds) {
        if (CollectionUtils.isEmpty(playgroupIds)) {
            return "FROM [Play] PL WITH (NOLOCK) WHERE PL.IsActive = 1 AND PL.IsVisible = 1 "
                    + "AND DATEDIFF(s,'19700101 00:00:00:000', PL.[Last_Modification_Date]) >= :start ";
        }

        return "FROM [Play] PL WITH (NOLOCK) JOIN [PlayGroupMap] PGM ON PL.Play_ID = PGM.Play_ID WHERE PL.IsActive = 1 AND PL.IsVisible = 1 "
                + "AND PGM.[PlayGroup_ID] IN (:playgroupIds) "
                + "AND DATEDIFF(s,'19700101 00:00:00:000', PL.[Last_Modification_Date]) >= :start ";
    }

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<Integer> accountIds) {
        String extensionColumns = getExtensionColumns();
        String sql = "SELECT * FROM (SELECT [Item_ID] AS ID ,"
                + "CASE WHEN A.CRMAccount_External_ID IS NOT NULL THEN A.CRMAccount_External_ID ELSE A.Alt_ID END AS SfdcAccountID, "
                + extensionColumns + " "
                + "DATEDIFF(s,'19700101 00:00:00:000', A.[Last_Modification_Date]) AS LastModificationDate, "
                + "ROW_NUMBER() OVER ( ORDER BY A.[Last_Modification_Date], [Item_ID]) RowNum "
                + getAccountExtensionFromWhereClause(accountIds)
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);
        if (!CollectionUtils.isEmpty(accountIds)) {
            source.addValue("accountIds", accountIds);
        }

        List<Map<String, Object>> result = queryForListOfMap(sql, source);
        if (result != null) {
            for (Map<String, Object> map : result) {
                map.remove("Item_ID");
            }
        }
        return result;
    }

    private String getExtensionColumns() {
        List<Map<String, Object>> schema = getAccountExtensionSchema();
        StringBuilder builder = new StringBuilder();
        for (Map<String, Object> field : schema) {
            builder.append("E.").append(field.get("Field")).append(", ");
        }

        return builder.toString();
    }

    @Override
    public int getAccountExtensionCount(long start, List<Integer> accountIds) {
        String sql = "SELECT COUNT(*) " + getAccountExtensionFromWhereClause(accountIds);
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(accountIds)) {
            source.addValue("accountIds", accountIds);
        }
        return queryForObject(sql, source, Integer.class);
    }

    private String getAccountExtensionFromWhereClause(List<Integer> accountIds) {
        String whereClause = "FROM [LEAccount_Extensions] E WITH (NOLOCK) JOIN [LEAccount] A WITH (NOLOCK) ON E.Item_ID = A.LEAccount_ID AND A.IsActive = 1 "
                + "%s " + "WHERE DATEDIFF(s,'19700101 00:00:00:000', A.[Last_Modification_Date]) >= :start ";

        StringBuilder extraFilter = new StringBuilder();
        if (CollectionUtils.isEmpty(accountIds)) {
            extraFilter.append("");
        } else {
            extraFilter.append("AND E.[Item_ID] IN (:accountIds) ");
        }
        return String.format(whereClause, extraFilter.toString());
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema() {
        String sql = "SELECT C.Column_Name AS Field, C.Column_Type AS Type, C.String_Length AS StringLength, "
                + "S.Value AS DisplayName "
                + "FROM [ExtensionColumnSpec] C JOIN [ExtensionTableSpec] T ON C.Parent_ID = T.ExtensionTableSpec_ID "
                + "AND T.External_ID = 'LEAccount' "
                + "JOIN [ConfigTableColumn] CC on C.Column_Name = CC.Column_Lookup_ID "
                + "JOIN [ConfigTable] CT ON CC.[ConfigTable_ID] = CT.ConfigTable_ID "
                + "JOIN [ConfigResource] S ON CC.Column_Display_Key = S.Key_Name AND S.Locale_ID = -1 "
                + "WHERE CC.IsActive = 1 and CC.Column_IsVisible = 1 and CT.External_ID = 'Sales-AccountList'";

        MapSqlParameterSource source = new MapSqlParameterSource();
        List<Map<String, Object>> result = queryForListOfMap(sql, source);
        return result;
    }

    @Override
    public int getAccountExtensionColumnCount() {
        List<Map<String, Object>> schema = getAccountExtensionSchema();
        return schema.size();
    }

    @Override
    public List<Map<String, Object>> getPlayValues(long start, int offset, int maximum, List<Integer> playgroupIds) {
        String sql = "SELECT * FROM (SELECT PL.[Play_ID] AS ID, "
                + "(SELECT DISTINCT G.Display_Name + '|' as [text()] FROM PlayGroupMap M JOIN PlayGroup G "
                + "ON M.PlayGroup_ID = G.PlayGroup_ID WHERE M.Play_ID = PL.Play_ID FOR XML PATH ('')) AS PlayGroups, "
                + "(SELECT DISTINCT W.Display_Name + '|' as [text()] FROM PlayWorkflowType W "
                + "WHERE W.PlayWorkflowType_ID = PL.PlayWorkflowType_ID FOR XML PATH ('')) AS Workflows, "
                + "(SELECT DISTINCT S.value + '|' as [text()] FROM [PlayPriorityRuleMap] M "
                + "JOIN [Priority] P ON M.Priority_ID = P.Priority_ID JOIN ConfigResource S ON P.[Display_Text_Key] = S.Key_Name "
                + "WHERE M.Play_ID = PL.Play_ID FOR XML PATH (''))  AS Priorities, "
                + "ROW_NUMBER() OVER ( ORDER BY PL.[Last_Modification_Date], [Play_ID]) RowNum "
                + getPlayFromWhereClause(playgroupIds)
                + " ) AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertToList("PlayGroups", results);
        convertToList("Workflows", results);
        convertToList("Priorities", results);
        if (!CollectionUtils.isEmpty(playgroupIds)) {
            source.addValue("playgroupIds", playgroupIds);
        }
        return results;
    }

    @Override
    public int getPlayValueCount(long start, List<Integer> playgroupIds) {
        String sql = "SELECT COUNT(*) " + getPlayFromWhereClause(playgroupIds);

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(playgroupIds)) {
            source.addValue("playgroupIds", playgroupIds);
        }
        return queryForObject(sql, source, Integer.class);
    }

    @Override
    public List<Map<String, Object>> getWorkflowTypes() {
        String sql = "SELECT External_ID as ID, Display_Name AS DisplayName FROM PlayWorkflowType WHERE IsActive = 1";

        MapSqlParameterSource source = new MapSqlParameterSource();
        return queryForListOfMap(sql, source);
    }

    @Override
    public List<Map<String, Object>> getPlayGroups(long start, int offset, int maximum) {
        String sql = "SELECT * FROM (SELECT [PlayGroup_ID] AS ID, External_ID AS ExternalID, Display_Name AS DisplayName, "
                + "DATEDIFF(s,'19700101 00:00:00:000', PlayGroup.[Last_Modification_Date]) AS LastModificationDate, "
                + "ROW_NUMBER() OVER ( ORDER BY PlayGroup.[Last_Modification_Date], PlayGroup.[PlayGroup_ID]) RowNum "
                + getPlayGroupWhereClause()
                + " ) AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";
        ;

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);
        return queryForListOfMap(sql, source);
    }

    @Override
    public int getPlayGroupCount(long start) {
        String sql = "SELECT COUNT(*) " + getPlayGroupWhereClause();

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        return queryForObject(sql, source, Integer.class);
    }

    private String getPlayGroupWhereClause() {
        return "FROM PlayGroup WHERE IsActive = 1 AND DATEDIFF(s,'19700101 00:00:00:000', PlayGroup.[Last_Modification_Date]) >= :start ";
    }
}
