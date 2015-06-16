package com.latticeengines.playmaker.dao.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.playmaker.dao.PalymakerRecommendationDao;

public class PalymakerRecommendationDaoImpl extends BaseGenericDaoImpl implements PalymakerRecommendationDao {

    public PalymakerRecommendationDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum) {
        String sql = "SELECT L.[PreLead_ID] AS ID, "
                + "L.[Display_Name] AS DisplayName, L.[Description] AS Description, A.Alt_ID AS SfdcAccountID, "
                + "L.[Play_ID] AS PlayID, DATEDIFF(s,'19700101 00:00:00:000', R.Start) AS LaunchDate, L.[Likelihood] AS Likelihood, "
                + "C.Value AS PriorityDisplayName, P.Priority_ID AS PriorityID, DATEDIFF(s,'19700101 00:00:00:000', L.[Expiration_Date]) AS ExpirationDate, "
                + "L.[Monetary_Value] AS MonetaryValue, M.ISO4217_ID AS MonetaryValueIso4217ID, "
                + "(SELECT TOP 1 T.[Display_Name] + '|' + T.[Phone_Number] + '|' + T.[Email_Address] + '|' + "
                + " T.[Address_Street_1] + '|' + T.[City] + '|' + T.[State_Province] + '|' + T.[Country]+ '|' + T.[Zip] "
                + "FROM [LEContact] T WHERE T.Account_ID = A.LEAccount_ID) AS Contacts, "
                + "DATEDIFF(s,'19700101 00:00:00:000', L.[Last_Modification_Date]) AS LastModificationDate "
                + getRecommendationFromWhereClause()
                + "ORDER BY LastModificationDate OFFSET :offset ROWS FETCH NEXT :maximum ROWS ONLY";

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("offset", offset);
        source.addValue("maximum", maximum);

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertContacts(results);

        return results;
    }

    private void convertContacts(List<Map<String, Object>> results) {
        if (CollectionUtils.isNotEmpty(results)) {
            for (Map<String, Object> record : results) {
                String contacts = (String) record.get("Contacts");
                if (contacts != null) {
                    String[] contactArray = contacts.split("[|]");
                    if (contactArray.length >= 8) {
                        Map<String, Object> contactMap = new HashMap<>();
                        contactMap.put("Name", contactArray[0]);
                        contactMap.put("Phone", contactArray[1]);
                        contactMap.put("Email", contactArray[2]);
                        contactMap.put("Address", contactArray[3]);
                        contactMap.put("City", contactArray[4]);
                        contactMap.put("State", contactArray[5]);
                        contactMap.put("Country", contactArray[6]);
                        contactMap.put("ZipCode", contactArray[7]);
                        record.put("Contacts", contactMap);
                    }

                }
            }
        }
    }

    @Override
    public int getRecommendationCount(long start) {
        String sql = "SELECT COUNT(*) " + getRecommendationFromWhereClause();

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        return queryForObject(sql, source, Integer.class);
    }

    private String getRecommendationFromWhereClause() {
        return "FROM [PreLead] L LEFT OUTER JOIN LaunchRun R "
                + "ON L.[LaunchRun_ID] = R.[LaunchRun_ID] JOIN LEAccount A "
                + "ON L.Account_ID = A.LEAccount_ID JOIN Priority P "
                + "ON L.Priority_ID = P.Priority_ID JOIN ConfigResource C "
                + "ON P.Display_Text_Key = C.Key_Name JOIN Currency M "
                + "ON L.[Monetary_Value_Currency_ID] = M.Currency_ID WHERE DATEDIFF(s,'19700101 00:00:00:000',L.[Last_Modification_Date]) >= :start ";
    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum) {
        String sql = "SELECT [Play_ID] AS ID, [Display_Name] AS DisplayName, "
                + "[Description] AS Description, [Average_Probability] AS AverageProbability,"
                + "DATEDIFF(s,'19700101 00:00:00:000', [Last_Modification_Date]) AS LastModificationDate, "
                + "(SELECT DISTINCT G.Display_Name + '|' as [text()] FROM PlayGroupMap M JOIN PlayGroup G "
                + "ON M.PlayGroup_ID = G.PlayGroup_ID WHERE M.Play_ID = Play.Play_ID FOR XML PATH ('')) AS PlayGroups, "
                + "(SELECT DISTINCT P.Display_Name + '|' as [text()] "
                + "FROM [ProductGroupMap] M JOIN Product P ON M.Product_ID = P.Product_ID "
                + "WHERE M.[ProductGroup_ID] = Play.[Target_ProductGroup_ID] FOR XML PATH ('')) AS TargetProducts "
                + getPlayFromWhereClause()
                + "ORDER BY Last_Modification_Date OFFSET :offset ROWS FETCH NEXT :maximum ROWS ONLY";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("offset", offset);
        source.addValue("maximum", maximum);

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertToList("PlayGroups", results);
        convertToList("TargetProducts", results);
        return results;
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
        return "FROM [Play] WHERE DATEDIFF(s,'19700101 00:00:00:000', [Last_Modification_Date]) >= :start ";
    }

    @Override
    public List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum) {
        String sql = "SELECT [Item_ID] AS ID, E.* " + getAccountExtensionFromWhereClause()
                + "ORDER BY Last_Modification_Date OFFSET :offset ROWS FETCH NEXT :maximum ROWS ONLY";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("offset", offset);
        source.addValue("maximum", maximum);

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
        return "FROM [LEAccount_Extensions] E JOIN [LEAccount] A ON E.Item_ID = A.LEAccount_ID WHERE DATEDIFF(s,'19700101 00:00:00:000', A.[Last_Modification_Date]) >= :start ";
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
        String sql = "SELECT [Play_ID] AS ID, "
                + "(SELECT DISTINCT G.Display_Name + '|' as [text()] FROM PlayGroupMap M JOIN PlayGroup G "
                + "ON M.PlayGroup_ID = G.PlayGroup_ID WHERE M.Play_ID = Play.Play_ID FOR XML PATH ('')) AS PlayGroups, "
                + "(SELECT DISTINCT S.value + '|' as [text()] FROM [PlayPriorityRuleMap] M "
                + "JOIN [Priority] P ON M.Priority_ID = P.Priority_ID JOIN ConfigResource S ON P.[Display_Text_Key] = S.Key_Name "
                + "WHERE M.Play_ID = Play.Play_ID FOR XML PATH (''))  AS Priorities " + getPlayFromWhereClause()
                + "ORDER BY Last_Modification_Date OFFSET :offset ROWS FETCH NEXT :maximum ROWS ONLY";

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("offset", offset);
        source.addValue("maximum", maximum);

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        convertToList("PlayGroups", results);
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
}
