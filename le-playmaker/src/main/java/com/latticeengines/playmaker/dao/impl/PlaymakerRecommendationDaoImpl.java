package com.latticeengines.playmaker.dao.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.playmaker.dao.PlaymakerRecommendationDao;

public class PlaymakerRecommendationDaoImpl extends BaseGenericDaoImpl implements PlaymakerRecommendationDao {

    private static final String DATEDIFF_1970 = "DATEDIFF(s,'19700101 00:00:00:000',";
    private static final String ID_KEY = "ID";
    private static final String SFDC_ACC_ID_KEY = "SfdcAccountID";
    private static final String SFDC_CONT_ID_KEY = "SfdcContactID";
    private static final String LAST_MODIFIED_TIME_KEY = "LastModificationDate";

    public PlaymakerRecommendationDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> idStrList, Map<String, String> orgInfo, Map<String, String> appId) {
        List<Integer> playIds = idStrListToIntList(idStrList);

        String sql = "SELECT * FROM (SELECT L.[PreLead_ID] AS ID, L.Account_ID AS AccountID, L.[LaunchRun_ID] AS LaunchID, "
                + "PL.[Display_Name] AS DisplayName, A.Display_Name AS CompanyName, A.External_ID AS LEAccountExternalID, "
                + "COALESCE(PL.[Description], L.[Description]) AS Description, "
                + "CASE WHEN A.CRMAccount_External_ID IS NOT NULL THEN A.CRMAccount_External_ID ELSE A.Alt_ID END AS SfdcAccountID, "
                + "L.[Play_ID] AS PlayID, " + DATEDIFF_1970 + " R.Start) AS LaunchDate, " + getLikelihood()
                + "C.Value AS PriorityDisplayName, P.Priority_ID AS PriorityID, "
                + "CASE WHEN L.[Expiration_Date] > '2030-03-15' THEN 1899763200 ELSE " + DATEDIFF_1970
                + " L.[Expiration_Date]) END AS ExpirationDate, " + getMonetaryValue()
                + "M.ISO4217_ID AS MonetaryValueIso4217ID, "
                + "(SELECT TOP 1  ISNULL(T.[Display_Name], '') + '|' + ISNULL(T.[Phone_Number], '') + '|' + ISNULL(T.[Email_Address], '') "
                + " + '|' +  ISNULL(T.[Address_Street_1], '') + '|' + ISNULL(T.[City], '') + '|' + ISNULL(T.[State_Province], '') "
                + " + '|' + ISNULL(T.[Country], '') + '|' + ISNULL(T.[Zip], '') + '|' + ISNULL(CONVERT(VARCHAR, T.[LEContact_ID]), '') "
                + getSfdcContactID() + "FROM [LEContact] T WHERE T.Account_ID = A.LEAccount_ID) AS Contacts, "
                + DATEDIFF_1970 + " L.[Last_Modification_Date]) AS LastModificationDate, "
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
        return " + '|' + COALESCE(T.[CrmLink_ID], '') ";
    }

    protected String getSfdcContactIDFromLEContact() {
        return "[CrmLink_ID] AS SfdcContactID, ";
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
                    if (contactArray.length >= 10) {
                        // On recommendation record as well.
                        record.put("SfdcContactID", contactArray[9]);

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
                        contactMap.put("ContactID", contactArray[8]);
                        contactMap.put("SfdcContactID", contactArray[9]);
                        contactList.add(contactMap);
                        record.put("Contacts", contactList);

                    }

                }
            }
        }
    }

    @Override
    public long getRecommendationCount(long start, int syncDestination, List<String> idStrList,
            Map<String, String> orgInfo, Map<String, String> appId) {
        List<Integer> playIds = idStrListToIntList(idStrList);

        String sql = "SELECT COUNT(*) " + getRecommendationFromWhereClause(syncDestination, playIds);

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("syncDestination", syncDestination);
        if (!CollectionUtils.isEmpty(playIds)) {
            source.addValue("playIds", playIds);
        }
        return queryForObject(sql, source, Long.class);
    }

    protected String getRecommendationFromWhereClause(int syncDestination, List<Integer> playIds) {
        String whereClause = "FROM [PreLead] L WITH (NOLOCK) LEFT OUTER JOIN LaunchRun R WITH (NOLOCK) "
                + "ON L.[LaunchRun_ID] = R.[LaunchRun_ID]  AND R.Launch_Stage = 0 JOIN LEAccount A WITH (NOLOCK) "
                + "ON L.Account_ID = A.LEAccount_ID AND A.IsActive = 1 JOIN Play PL "
                + "ON L.Play_ID = PL.Play_ID AND PL.IsActive = 1 AND PL.IsVisible = 1 JOIN Priority P WITH (NOLOCK) "
                + "ON L.Priority_ID = P.Priority_ID JOIN ConfigResource C WITH (NOLOCK) "
                + "ON P.Display_Text_Key = C.Key_Name AND C.Locale_ID = -1 JOIN Currency M WITH (NOLOCK) "
                + "ON L.[Monetary_Value_Currency_ID] = M.Currency_ID " + "WHERE L.Status = 2800 AND L.IsActive = 1 AND "
                + "L.Synchronization_Destination in (" + getDestinationonValues(syncDestination) + ") " + "%s " + "AND "
                + DATEDIFF_1970 + "L.[Last_Modification_Date]) >= :start ";

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
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds,
            int syncDestination, Map<String, String> orgInfo) {
        String sql = "SELECT * FROM (SELECT PL.[Play_ID] AS ID, PL.[External_ID] AS ExternalID, PL.[Display_Name] AS DisplayName, "
                + "PL.[Description] AS Description, " + "PL.[Average_Probability] AS AverageProbability,"
                + DATEDIFF_1970 + " PL.[Last_Modification_Date]) AS LastModificationDate, "
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
    public long getPlayCount(long start, List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo) {
        String sql = "SELECT COUNT(*) " + getPlayFromWhereClause(playgroupIds);
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(playgroupIds)) {
            source.addValue("playgroupIds", playgroupIds);
        }
        return queryForObject(sql, source, Long.class);
    }

    private String getPlayFromWhereClause(List<Integer> playgroupIds) {
        if (CollectionUtils.isEmpty(playgroupIds)) {
            return "FROM [Play] PL WITH (NOLOCK) WHERE PL.IsActive = 1 AND PL.IsVisible = 1 " + "AND " + DATEDIFF_1970
                    + " PL.[Last_Modification_Date]) >= :start ";
        }

        return "FROM [Play] PL WITH (NOLOCK) JOIN [PlayGroupMap] PGM ON PL.Play_ID = PGM.Play_ID WHERE PL.IsActive = 1 AND PL.IsVisible = 1 "
                + "AND PGM.[PlayGroup_ID] IN (:playgroupIds) " + "AND " + DATEDIFF_1970
                + " PL.[Last_Modification_Date]) >= :start ";
    }

    @Override
    public List<Map<String, Object>> getAccountExtensions(Long start, int offset, int maximum, List<String> idStrList,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId, Map<String, String> orgInfo) {
        return getAccountExtensions(start, offset, maximum, filterBy, recStart, columns, hasSfdcContactId, idStrList);
    }

    private List<Map<String, Object>> getAccountExtensions(Long start, int offset, int maximum, String filterBy,
            Long recStart, String columns, boolean hasSfdcContactId, List<String> accountIds) {
        boolean hasValidFilterBy = false;
        filterBy = (filterBy == null ? null : filterBy.trim().toUpperCase());
        if (StringUtils.isNotEmpty(filterBy)
                && (filterBy.equals("RECOMMENDATIONS") || filterBy.equals("NORECOMMENDATIONS"))) {
            if (recStart == null) {
                throw new RuntimeException("Missng recStart when filterBy is used.");
            }
            hasValidFilterBy = true;
        }

        String additionalColumns = getAccountExtensionColumns(columns);
        if (!StringUtils.isBlank(additionalColumns)) {
            additionalColumns = additionalColumns.trim();
            if (!additionalColumns.endsWith(",")) {
                additionalColumns = additionalColumns + ",";
            }
        } else {
            additionalColumns = "";
        }

        String sqlStr = //
                " DECLARE @startdate int = " + (hasValidFilterBy ? ":recStart" : ":start") + "; " //
                        + " DECLARE @startrow int = (:offset + 1); " //
                        + " DECLARE @endrow int = (:offset + :maximum) ; " //
                        + "  " //
                        + " DECLARE @rowcount int; " //
                        + " DECLARE @first_id int; " //
                        + " DECLARE @first_lmd datetime; " //
                        + "  " //
                        + " --if startrow is zero or less set it to 1 this will prevent  \n" //
                        + " --issues later when @rowcount is calculated \n" //
                        + " IF @startrow <= 0 " //
                        + " BEGIN " //
                        + "     SET @startrow = 1 " //
                        + " END " //
                        + "  " //
                        + " --return at most @startrow number of records and assign  \n" //
                        + " --the values of the last row returned to @first_lmd and @first_id \n" //
                        + " SET ROWCOUNT @startrow; " //
                        + " SELECT @first_lmd = Last_Modification_Date, @first_id = LEAccount_ID " //
                        + " FROM   LEAccount " //
                        + " WHERE  [Last_Modification_Date] >= DATEADD(s, @startdate, '1970-01-01 00:00:00') "
                        + " AND    IsActive = 1 " //
                        + (hasValidFilterBy ? (" AND LEAccount_ID " + getInNotInClauseForFilterBy(filterBy)) : "")
                        + " ORDER BY Last_Modification_Date, LEAccount_ID; " //
                        + "      " //
                        + " --only query for records if @startrow is less than or equal to the \n" //
                        + " --number of rows actually returned (@@ROWCOUNT) \n" //
                        + " IF @startrow <= @@ROWCOUNT " //
                        + " BEGIN " //
                        + "     PRINT @first_lmd " //
                        + "     PRINT @first_id " //
                        + "  " //
                        + "     SET @rowcount = (@endrow - @startrow) + 1; " //
                        + "     SET ROWCOUNT @rowcount; " //
                        + "     SELECT " //
                        + "     A.External_ID AS LEAccountExternalID, " //
                        + "     E.Item_ID AS " + ID_KEY + ", " //
                        + " " + additionalColumns //
                        + " " + getSfdcAccountContactIds(hasSfdcContactId) //
                        + "     DATEDIFF(s, '19700101 00:00:00:000', A.[Last_Modification_Date]) AS " //
                        + " " + LAST_MODIFIED_TIME_KEY //
                        + " " + getAccountExtensionFromWhereClause(accountIds, filterBy, //
                                false, start == null)
                        + "; " + "  " //
                        + " END " //
                        + " ELSE " //
                        + " BEGIN " //
                        + "     SELECT TOP 0 NULL " //
                        + " END " //
                        + " " //
                        + " SET ROWCOUNT 0; ";

        MapSqlParameterSource source = new MapSqlParameterSource();

        if (hasValidFilterBy) {
            source.addValue("recStart", recStart);
        }
        source.addValue("start", start);
        source.addValue("offset", offset);
        source.addValue("maximum", maximum);
        if (!CollectionUtils.isEmpty(accountIds)) {
            List<Integer> aIds = idStrListToIntList(accountIds);
            source.addValue("accountIds", aIds);
        }

        List<Map<String, Object>> result = queryNativeSql(sqlStr, source);

        if (CollectionUtils.isNotEmpty(result)) {
            int rowNum = 1;
            for (Map<String, Object> res : result) {
                res.put("RowNum", offset + rowNum++);
            }
        }
        return result;
    }

    private String getSfdcAccountContactIds(boolean hasSfdcContactId) {
        if (hasSfdcContactId) {
            return "CASE WHEN A.External_ID IS NOT NULL AND A.External_ID like '001%' THEN A.External_ID ELSE NULL END AS "
                    + SFDC_ACC_ID_KEY + ", "
                    + "CASE WHEN A.External_ID IS NOT NULL AND A.External_ID like '003%' THEN A.External_ID ELSE NULL END AS "
                    + SFDC_CONT_ID_KEY + ", ";
        } else {
            return "CASE WHEN A.CRMAccount_External_ID IS NOT NULL THEN A.CRMAccount_External_ID ELSE A.Alt_ID END AS "
                    + SFDC_ACC_ID_KEY + ", " + " NULL AS " + SFDC_CONT_ID_KEY + ", ";
        }
    }

    private String getAccountExtensionColumns(String columns) {
        if (columns != null) {
            columns = StringUtils.strip(columns);
            if ("".equals(columns)) {
                return "";
            }
        }

        List<Map<String, Object>> schema = getAccountExtensionSchema(null);
        StringBuilder builder = new StringBuilder();
        Set<String> columnsInDb = new HashSet<>();
        for (Map<String, Object> field : schema) {
            builder.append("E.").append(field.get("Field")).append(" as ").append(field.get("Field")).append(", ");
            columnsInDb.add(field.get("Field").toString());
        }
        if (columns == null) {
            return builder.toString();
        }
        return getSelectedColumns(columns, columnsInDb);

    }

    private String getSelectedColumns(String selectedColumns, Set<String> columnsInDb) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotEmpty(selectedColumns)) {
            selectedColumns = selectedColumns.trim();
            String[] columns = StringUtils.split(selectedColumns, ",");
            for (String column : columns) {
                column = column.trim();
                if (StringUtils.isNotEmpty(column) && columnsInDb.contains(column)) {
                    builder.append("E.").append(column).append(" as ").append(column).append(", ");
                }
            }
        }
        if (builder.length() > 0) {
            return builder.toString();
        }
        return "";
    }

    @Override
    public long getAccountExtensionCount(Long start, List<String> idStrList, String filterBy, Long recStart,
            Map<String, String> orgInfo) {
        String sql = "SELECT COUNT(*) " + getAccountExtensionFromWhereClause(idStrList, filterBy, true, true);
        MapSqlParameterSource source = new MapSqlParameterSource();
        if (StringUtils.isNotEmpty(filterBy) && (filterBy.toUpperCase().equals("RECOMMENDATIONS")
                || filterBy.toUpperCase().equals("NORECOMMENDATIONS"))) {
            if (recStart == null) {
                throw new RuntimeException("Missng recStart when filterBy is used.");
            }
            source.addValue("recStart", recStart);
        }
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(idStrList)) {
            List<Integer> aIds = idStrListToIntList(idStrList);
            source.addValue("accountIds", aIds);
        }

        return queryForObject(sql, source, Long.class);
    }

    private String getAccountExtensionFromWhereClause(List<String> accountIds, String filterBy, boolean forCount,
            boolean isIntegerTimestamp) {
        String fromAndWhereClause = null;
        if (StringUtils.isNotEmpty(filterBy)) {
            filterBy = filterBy.trim().toUpperCase();
        }

        if (StringUtils.isNotEmpty(filterBy) && (filterBy.toUpperCase().equals("RECOMMENDATIONS")
                || filterBy.toUpperCase().equals("NORECOMMENDATIONS"))) {
            fromAndWhereClause = getAccountExtensionFromWhereClauseWithFilterBy(filterBy);
        } else {
            fromAndWhereClause = getAccountExtensionFromClause();
            if (!CollectionUtils.isEmpty(accountIds)) {
                List<Integer> aIds = idStrListToIntList(accountIds);
                fromAndWhereClause += " WHERE %s ";
                fromAndWhereClause = String.format(fromAndWhereClause, getAndClauseForAccountIds(aIds));
            }
        }

        fromAndWhereClause += " %s ";
        return String.format(fromAndWhereClause, //
                forCount ? //
                        " AND (" + DATEDIFF_1970 + " " + getAccountLastModificationDate() + " ) >=  " + ":start )  "
                        : " AND ( " //
                                + " ( A.Last_Modification_Date = @first_lmd AND A.LEAccount_ID >= @first_id) " //
                                + "   OR " //
                                + "   A.Last_Modification_Date > @first_lmd ) " //
                                + " ORDER BY A.Last_Modification_Date, A.LEAccount_ID");
    }

    private String getAccountLastModificationDate() {
        return " A.[Last_Modification_Date] ";
    }

    private String getAndClauseForAccountIds(List<Integer> accountIds) {
        if (!CollectionUtils.isEmpty(accountIds)) {
            return " A.[LEAccount_ID] IN (:accountIds) ";
        }
        return "";
    }

    private String getAccountExtensionFromClause() {
        return "FROM [LEAccount_Extensions] E WITH (NOLOCK) JOIN [LEAccount] A WITH (NOLOCK) ON E.Item_ID = A.LEAccount_ID AND A.IsActive = 1 ";
    }

    private String getAccountExtensionFromWhereClauseWithFilterBy(String filterBy) {
        String whereClause = " WHERE E.Item_ID " + getInNotInClauseForFilterBy(filterBy);
        return getAccountExtensionFromClause() + whereClause;
    }

    private String getInNotInClauseForFilterBy(String filterBy) {
        String whereClause = " %s (SELECT L.Account_ID FROM [Prelead] L WHERE L.Status = 2800 AND L.IsActive = 1 AND "
                + DATEDIFF_1970 + " L.[Last_Modification_Date]) >= :recStart) ";
        String oper = filterBy.equals("RECOMMENDATIONS") ? "IN" : "NOT IN";
        whereClause = String.format(whereClause, oper);
        return whereClause;
    }

    @Override
    public List<Map<String, Object>> getAccountExtensionSchema(String customerSpace) {
        String sql = "SELECT C.Column_Name AS Field, C.Column_Type AS Type, C.String_Length AS StringLength, "
                + "S.Value AS DisplayName "
                + "FROM [ExtensionColumn] C JOIN [ExtensionTable] T ON C.Parent_ID = T.ExtensionTable_ID "
                + "AND T.Table_Notion = 'LEAccount' "
                + "JOIN [ConfigTableColumn] CC on C.Column_Name = CC.Column_Lookup_ID "
                + "JOIN [ConfigTable] CT ON CC.[ConfigTable_ID] = CT.ConfigTable_ID "
                + "JOIN [ConfigResource] S ON CC.Column_Display_Key = S.Key_Name AND S.Locale_ID = -1 "
                + "WHERE CC.IsActive = 1 and CT.External_ID = 'Sales-AccountList'";

        MapSqlParameterSource source = new MapSqlParameterSource();
        List<Map<String, Object>> result = queryForListOfMap(sql, source);
        return result;
    }

    @Override
    public long getAccountExtensionColumnCount(String customerSpace) {
        List<Map<String, Object>> schema = getAccountExtensionSchema(customerSpace);
        return schema.size();
    }

    @Override
    public List<Map<String, Object>> getContacts(long start, int offset, int maximum, List<String> contactIds,
            List<String> accountIds, Long recStart, List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId) {
        String sql = "SELECT * FROM (SELECT C.[LEContact_ID] AS ID, C.[External_ID] AS ExternalID, C.[Display_Name] AS Name, C.[Account_ID] AS AccountID, "
                + "C.[Description] AS Description, C.[Title] AS Title, C.[Phone_Number] AS Phone, "
                + "C.[Fax_Number] AS Fax, C.[Email_Address] AS Email,"
                + "C.[Address_Street_1] AS Address, C.[Address_Street_2] AS Address2,"
                + "C.[City] AS City, C.[State_Province] AS State," + "C.[Country] AS Country, C.[Zip] AS ZipCode,"
                + getSfdcContactIDFromLEContact() + DATEDIFF_1970
                + " C.[Last_Modification_Date]) AS LastModificationDate, "
                + "ROW_NUMBER() OVER ( ORDER BY C.[Last_Modification_Date], C.[LEContact_ID] ) RowNum "
                + getContactFromWhereClause(contactIds, accountIds)
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);
        if (!CollectionUtils.isEmpty(contactIds)) {
            List<Integer> cIds = idStrListToIntList(contactIds);
            source.addValue("contactIds", cIds);
        }
        if (!CollectionUtils.isEmpty(accountIds)) {
            List<Integer> aIds = idStrListToIntList(accountIds);
            source.addValue("accountIds", aIds);
        }

        List<Map<String, Object>> results = queryForListOfMap(sql, source);
        return results;
    }

    @Override
    public long getContactCount(long start, List<String> contactIds, List<String> accountIds, Long recStart,
            List<String> playIds, Map<String, String> orgInfo, Map<String, String> appId) {
        String sql = "SELECT COUNT(*) " + getContactFromWhereClause(contactIds, accountIds);
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(contactIds)) {
            List<Integer> cIds = idStrListToIntList(contactIds);
            source.addValue("contactIds", cIds);
        }
        if (!CollectionUtils.isEmpty(accountIds)) {
            List<Integer> aIds = idStrListToIntList(accountIds);
            source.addValue("accountIds", aIds);
        }
        return queryForObject(sql, source, Long.class);
    }

    private String getContactFromWhereClause(List<String> contactIds, List<String> accountIds) {
        String whereClause = "FROM [LEContact] C WITH (NOLOCK) WHERE C.IsActive = 1 " + "%s AND " + DATEDIFF_1970
                + " C.[Last_Modification_Date]) >= :start ";

        StringBuilder extraFilter = new StringBuilder();
        if (CollectionUtils.isEmpty(contactIds)) {
            extraFilter.append("");
        } else {
            extraFilter.append("AND C.[LEContact_ID] IN (:contactIds) ");
        }
        if (CollectionUtils.isEmpty(accountIds)) {
            extraFilter.append("");
        } else {
            extraFilter.append("AND C.[Account_ID] IN (:accountIds) ");
        }

        return String.format(whereClause, extraFilter.toString());
    }

    @Override
    public List<Map<String, Object>> getContactExtensions(long start, int offset, int maximum, List<String> contactIds,
            Long recStart, Map<String, String> orgInfo, Map<String, String> appId) {
        String extensionColumns = getContactExtensionColumns();
        String sql = "SELECT * FROM (SELECT [Item_ID] AS ID, " + getSfdcContactIDFromLEContact() + extensionColumns
                + " " + DATEDIFF_1970 + " C.[Last_Modification_Date]) AS LastModificationDate, "
                + "ROW_NUMBER() OVER ( ORDER BY C.[Last_Modification_Date], [Item_ID]) RowNum "
                + getContactExtensionFromWhereClause(contactIds)
                + ") AS output WHERE RowNum >= :startRow AND RowNum <= :endRow ORDER BY RowNum";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        source.addValue("startRow", offset + 1);
        source.addValue("endRow", offset + maximum);
        if (!CollectionUtils.isEmpty(contactIds)) {
            List<Integer> cIds = idStrListToIntList(contactIds);
            source.addValue("contactIds", cIds);
        }

        List<Map<String, Object>> result = queryForListOfMap(sql, source);
        if (result != null) {
            for (Map<String, Object> map : result) {
                map.remove("Item_ID");
            }
        }
        return result;
    }

    @Override
    public long getContactExtensionCount(long start, List<String> contactIds, Long recStart,
            Map<String, String> orgInfo, Map<String, String> appId) {
        String sql = "SELECT COUNT(*) " + getContactExtensionFromWhereClause(contactIds);
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(contactIds)) {
            List<Integer> cIds = idStrListToIntList(contactIds);
            source.addValue("contactIds", cIds);
        }
        return queryForObject(sql, source, Long.class);
    }

    private String getContactExtensionFromWhereClause(List<String> contactIds) {
        String whereClause = "FROM [LEContact_Extensions] E WITH (NOLOCK) JOIN [LEContact] C WITH (NOLOCK) ON E.Item_ID = C.LEContact_ID AND C.IsActive = 1 "
                + "%s " + "WHERE " + DATEDIFF_1970 + " C.[Last_Modification_Date]) >= :start ";

        StringBuilder extraFilter = new StringBuilder();
        if (CollectionUtils.isEmpty(contactIds)) {
            extraFilter.append("");
        } else {
            extraFilter.append("AND E.[Item_ID] IN (:contactIds) ");
        }
        return String.format(whereClause, extraFilter.toString());
    }

    @Override
    public List<Map<String, Object>> getContactExtensionSchema(String customerSpace) {
        String sql = "SELECT C.Column_Name AS Field, C.Column_Type AS Type, C.String_Length AS StringLength, "
                + "(SELECT DISTINCT S.value FROM ConfigResource S "
                + "WHERE C.Display_Name_Key IS NOT NULL AND S.Key_Name = C.Display_Name_Key AND S.Locale_ID = -1) AS DisplayName "
                + "FROM [ExtensionColumn] C JOIN [ExtensionTable] T ON C.Parent_ID = T.ExtensionTable_ID "
                + "AND T.Table_Notion = 'LEContact' " + "WHERE C.IsActive = 1 ";

        MapSqlParameterSource source = new MapSqlParameterSource();
        List<Map<String, Object>> result = queryForListOfMap(sql, source);
        return result;
    }

    private String getContactExtensionColumns() {
        List<Map<String, Object>> schema = getContactExtensionSchema(null);
        StringBuilder builder = new StringBuilder();
        for (Map<String, Object> field : schema) {
            builder.append("E.").append(field.get("Field")).append(", ");
        }

        return builder.toString();
    }

    @Override
    public long getContactExtensionColumnCount(String customerSpace) {
        List<Map<String, Object>> schema = getContactExtensionSchema(customerSpace);
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
    public long getPlayValueCount(long start, List<Integer> playgroupIds) {
        String sql = "SELECT COUNT(*) " + getPlayFromWhereClause(playgroupIds);

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        if (!CollectionUtils.isEmpty(playgroupIds)) {
            source.addValue("playgroupIds", playgroupIds);
        }
        return queryForObject(sql, source, Long.class);
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
                + DATEDIFF_1970 + " PlayGroup.[Last_Modification_Date]) AS LastModificationDate, "
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
    public long getPlayGroupCount(long start) {
        String sql = "SELECT COUNT(*) " + getPlayGroupWhereClause();

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("start", start);
        return queryForObject(sql, source, Long.class);
    }

    private String getPlayGroupWhereClause() {
        return "FROM PlayGroup WHERE IsActive = 1 AND " + DATEDIFF_1970
                + " PlayGroup.[Last_Modification_Date]) >= :start ";
    }

    private List<Integer> idStrListToIntList(List<String> idStrList) {
        if (idStrList == null) {
            return null;
        }

        List<Integer> ids = idStrList.stream()//
                .map(idStr -> {
                    Integer id = Integer.parseInt(idStr);
                    return id;
                })//
                .collect(Collectors.toList());
        return ids;
    }
}
