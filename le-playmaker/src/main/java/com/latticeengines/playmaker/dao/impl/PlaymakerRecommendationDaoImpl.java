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
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination) {
        String sql = "SELECT * FROM (SELECT L.[PreLead_ID] AS ID, L.Account_ID AS AccountID, L.[LaunchRun_ID] AS LaunchID, "
                + "PL.[Display_Name] AS DisplayName, A.Display_Name AS CompanyName, "
                + "CASE WHEN PL.[Description] IS NOT NULL THEN PL.[Description] ELSE L.[Description] END AS Description, "
                + "CASE WHEN A.CRMAccount_External_ID IS NOT NULL THEN A.CRMAccount_External_ID ELSE A.Alt_ID END AS SfdcAccountID, "
                + "L.[Play_ID] AS PlayID, DATEDIFF(s,'19700101 00:00:00:000', R.Start) AS LaunchDate, "
                + getLikelihood()
                + "C.Value AS PriorityDisplayName, P.Priority_ID AS PriorityID, " 
                + "CASE WHEN L.[Expiration_Date] > '2030-03-15' THEN 1899763200 ELSE DATEDIFF(s,'19700101 00:00:00:000', L.[Expiration_Date]) END AS ExpirationDate, "
                + getMonetaryValue() 
                + "M.ISO4217_ID AS MonetaryValueIso4217ID, "
                + "(SELECT TOP 1 T.[Display_Name] + '|' + T.[Phone_Number] + '|' + T.[Email_Address] + '|' + "
                + " T.[Address_Street_1] + '|' + T.[City] + '|' + T.[State_Province] + '|' + T.[Country]+ '|' + T.[Zip] "
                + "FROM [LEContact] T WHERE T.Account_ID = A.LEAccount_ID) AS Contacts, "
                + "DATEDIFF(s,'19700101 00:00:00:000', L.[Last_Modification_Date]) AS LastModificationDate, "
                + "ROW_NUMBER() OVER ( ORDER BY L.[Last_Modification_Date], L.[PreLead_ID]) RowNum "
                + getRecommendationFromWhereClause(syncDestination)
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);
        source.addValue("syncDestination", syncDestination);

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertContacts(results);

        return results;
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
                    String[] contactArray = contacts.split("[|]");
                    if (contactArray.length >= 8) {
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
                        contactList.add(contactMap);
                        record.put("Contacts", contactList);
                    }

                }
            }
        }
    }

    @Override
    public int getRecommendationCount(long start, int syncDestination) {
        String sql = "SELECT COUNT(*) " + getRecommendationFromWhereClause(syncDestination);

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("syncDestination", syncDestination);
        return queryForObject(sql, source, Integer.class);
    }

    protected String getRecommendationFromWhereClause(int syncDestination) {
        return "FROM [PreLead] L WITH (NOLOCK) LEFT OUTER JOIN LaunchRun R WITH (NOLOCK) "
                + "ON L.[LaunchRun_ID] = R.[LaunchRun_ID]  AND R.Launch_Stage = 0 JOIN LEAccount A WITH (NOLOCK) "
                + "ON L.Account_ID = A.LEAccount_ID JOIN Play PL "
                + "ON L.Play_ID = PL.Play_ID AND PL.IsActive = 1 AND PL.IsVisible = 1 JOIN Priority P WITH (NOLOCK) "
                + "ON L.Priority_ID = P.Priority_ID JOIN ConfigResource C WITH (NOLOCK) "
                + "ON P.Display_Text_Key = C.Key_Name AND C.Locale_ID = -1 JOIN Currency M WITH (NOLOCK) "
                + "ON L.[Monetary_Value_Currency_ID] = M.Currency_ID " + "WHERE L.Status = 2800 AND L.IsActive = 1 AND "
                + "L.Synchronization_Destination in (" + getDestinationonValues(syncDestination) + ") "
                + "AND DATEDIFF(s,'19700101 00:00:00:000',L.[Last_Modification_Date]) >= :start ";
    }

    private String getDestinationonValues(int syncDestination) {
        switch (syncDestination) {
        case 0:
            return "0,2";
        case 1:
            return "1,2";
        case 2:
            return "0,1,2";
        default:
            return "0,2";
        }
    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum) {
        String sql = "SELECT * FROM (SELECT [Play_ID] AS ID, [Display_Name] AS DisplayName, "
                + "[Description] AS Description, [Average_Probability] AS AverageProbability,"
                + "DATEDIFF(s,'19700101 00:00:00:000', [Last_Modification_Date]) AS LastModificationDate, "
                + "(SELECT DISTINCT G.Display_Name + '|' as [text()] FROM PlayGroupMap M JOIN PlayGroup G "
                + "ON M.PlayGroup_ID = G.PlayGroup_ID WHERE M.Play_ID = Play.Play_ID FOR XML PATH ('')) AS PlayGroups, "
                + "(SELECT DISTINCT P.Display_Name + '|' + P.[External_Name] + '|' as [text()] "
                + "FROM [ProductGroupMap] M JOIN Product P ON M.Product_ID = P.Product_ID "
                + "WHERE M.[ProductGroup_ID] = Play.[Target_ProductGroup_ID] FOR XML PATH ('')) AS TargetProducts, "
                + "(SELECT W.[External_ID] FROM [PlayWorkflowType] W WHERE Play.[PlayWorkflowType_ID] = W.[PlayWorkflowType_ID]) AS Workflow, "
                + "ROW_NUMBER() OVER ( ORDER BY [Last_Modification_Date] ) RowNum " + getPlayFromWhereClause()
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);

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
    public int getPlayCount(long start) {
        String sql = "SELECT COUNT(*) " + getPlayFromWhereClause();
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);

        return queryForObject(sql, source, Integer.class);
    }

    private String getPlayFromWhereClause() {
        return "FROM [Play] WITH (NOLOCK) WHERE Play.IsActive = 1 AND Play.IsVisible = 1 AND DATEDIFF(s,'19700101 00:00:00:000', [Last_Modification_Date]) >= :start ";
    }

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum) {
        String sql = "SELECT * FROM (SELECT [Item_ID] AS ID, E.*, "
                + "DATEDIFF(s,'19700101 00:00:00:000', A.[Last_Modification_Date]) AS LastModificationDate, "
                + "ROW_NUMBER() OVER ( ORDER BY A.[Last_Modification_Date] ) RowNum "
                + getAccountExtensionFromWhereClause()
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);

        List<Map<String, Object>> result = queryForListOfMap(sql, source);
        if (result != null) {
            for (Map<String, Object> map : result) {
                map.remove("Item_ID");
            }
        }
        return result;
    }

    @Override
    public int getAccountExtensionCount(long start) {
        String sql = "SELECT COUNT(*) " + getAccountExtensionFromWhereClause();
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        return queryForObject(sql, source, Integer.class);
    }

    private String getAccountExtensionFromWhereClause() {
        return "FROM [LEAccount_Extensions] E WITH (NOLOCK) JOIN [LEAccount] A WITH (NOLOCK) ON E.Item_ID = A.LEAccount_ID WHERE DATEDIFF(s,'19700101 00:00:00:000', A.[Last_Modification_Date]) >= :start ";
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema() {
        String sql = "SELECT C.Column_Name AS Field, C.Column_Type AS Type, C.String_Length AS StringLength, "
                + "(SELECT DISTINCT S.value FROM ConfigResource S JOIN [ExtensionColumnSpec] "
                + "ON S.Key_Name = C.Display_Name_Key) AS DisplayName "
                + "FROM [ExtensionColumnSpec] C JOIN [ExtensionTableSpec] T ON C.Parent_ID = T.ExtensionTableSpec_ID WHERE T.External_ID = 'LEAccount' ";

        MapSqlParameterSource source = new MapSqlParameterSource();
        List<Map<String, Object>> result = queryForListOfMap(sql, source);
        return result;
    }

    @Override
    public List<Map<String, Object>> getPlayValues(long start, int offset, int maximum) {
        String sql = "SELECT * FROM (SELECT [Play_ID] AS ID, "
                + "(SELECT DISTINCT G.Display_Name + '|' as [text()] FROM PlayGroupMap M JOIN PlayGroup G "
                + "ON M.PlayGroup_ID = G.PlayGroup_ID WHERE M.Play_ID = Play.Play_ID FOR XML PATH ('')) AS PlayGroups, "
                + "(SELECT DISTINCT W.Display_Name + '|' as [text()] FROM PlayWorkflowType W "
                + "WHERE W.PlayWorkflowType_ID = Play.PlayWorkflowType_ID FOR XML PATH ('')) AS Workflows, "
                + "(SELECT DISTINCT S.value + '|' as [text()] FROM [PlayPriorityRuleMap] M "
                + "JOIN [Priority] P ON M.Priority_ID = P.Priority_ID JOIN ConfigResource S ON P.[Display_Text_Key] = S.Key_Name "
                + "WHERE M.Play_ID = Play.Play_ID FOR XML PATH (''))  AS Priorities, "
                + "ROW_NUMBER() OVER ( ORDER BY [Last_Modification_Date] ) RowNum " + getPlayFromWhereClause()
                + " ) AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertToList("PlayGroups", results);
        convertToList("Workflows", results);
        convertToList("Priorities", results);
        return results;
    }

    @Override
    public int getPlayValueCount(long start) {
        String sql = "SELECT COUNT(*) " + getPlayFromWhereClause();

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        return queryForObject(sql, source, Integer.class);
    }

    @Override
    public List<Map<String, Object>> getWorkflowTypes() {
        String sql = "SELECT External_ID as ID, Display_Name AS DisplayName FROM PlayWorkflowType";

        MapSqlParameterSource source = new MapSqlParameterSource();
        return queryForListOfMap(sql, source);

    }
}
