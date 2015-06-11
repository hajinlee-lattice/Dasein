package com.latticeengines.playmaker.dao.impl;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.playmaker.dao.PalymakerRecommendationDao;

public class PalymakerRecommendationDaoImpl extends BaseGenericDaoImpl implements PalymakerRecommendationDao {

    public PalymakerRecommendationDaoImpl(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(int startId, int size) {
        String sql = "SELECT TOP "
                + size
                + " L.[PreLead_ID] AS ID, "
                + "L.[Display_Name] AS DisplayName, L.[Description] AS Description, A.Alt_ID AS SfdcAccountID, "
                + "L.[Play_ID] AS PlayID, DATEDIFF(s,'19700101 00:00:00:000', R.Start) AS LaunchDate, L.[Likelihood] AS Likelihood, "
                + "C.Value AS PriorityDisplayName, P.Priority_ID AS PriorityID, DATEDIFF(s,'19700101 00:00:00:000', L.[Expiration_Date]) AS ExpirationDate, "
                + "L.[Monetary_Value] AS MonetaryValue, M.ISO4217_ID AS MonetaryValueIso4217ID, "
                + "DATEDIFF(s,'19700101 00:00:00:000', L.[Last_Modification_Date]) AS LastModificationDate FROM [PreLead] L LEFT OUTER JOIN LaunchRun R "
                + "ON L.[LaunchRun_ID] = R.[LaunchRun_ID] JOIN LEAccount A "
                + "ON L.Account_ID = A.LEAccount_ID JOIN Priority P "
                + "ON L.Priority_ID = P.Priority_ID JOIN ConfigResource C "
                + "ON P.Display_Text_Key = C.Key_Name JOIN Currency M "
                + "ON L.[Monetary_Value_Currency_ID] = M.Currency_ID WHERE L.[PreLead_ID] >= :startId ORDER BY ID";

        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("startId", startId);

        return queryForListOfMap(sql, source);

    }

    @Override
    public List<Map<String, Object>> getPlays(int startId, int size) {
        String sql = "SELECT TOP "
                + size
                + " [Play_ID] AS ID, [Display_Name] AS DisplayName, "
                + "[Description] AS Description, [Average_Probability] AS AverageProbability,"
                + "DATEDIFF(s,'19700101 00:00:00:000', [Last_Modification_Date]) AS LastModificationDate, "
                + "(SELECT DISTINCT G.Display_Name + '|' as [text()] FROM PlayGroupMap M JOIN PlayGroup G "
                + "ON M.PlayGroup_ID = G.PlayGroup_ID WHERE M.Play_ID = Play.Play_ID FOR XML PATH ('')) AS PlayGroups, "
                + "(SELECT DISTINCT P.Display_Name + '|' as [text()] "
                + "FROM [ProductGroupMap] M JOIN Product P ON M.Product_ID = P.Product_ID "
                + "WHERE M.[ProductGroup_ID] = Play.[Target_ProductGroup_ID] FOR XML PATH ('')) AS TargetProducts "
                + "FROM [Play] WHERE [Play_ID] >= :startId ORDER BY ID";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("startId", startId);

        return queryForListOfMap(sql, source);
    }
}
