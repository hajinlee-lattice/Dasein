package com.latticeengines.query.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.kitesdk.shaded.com.google.common.collect.Lists;

import com.latticeengines.query.exposed.translator.TranslatorUtils;

public class SparkSQLQueryUtils {

    public static final String FINAL = "final";

    public static List<List<String>> detachSubQueries(String sql) {
        List<List<String>> queries = new ArrayList<>();
        int count = 0;
        while (count++ < 50) {
            if (count >= 50) {
                throw new UnsupportedOperationException("There are more than 50 sub-queries in the sql: " + sql);
            }
            Matcher matcher = Pattern.compile("with .* as ").matcher(sql);
            if (matcher.find()) {
                String phrase = matcher.group();
                phrase = phrase.substring(0, phrase.indexOf(" as ")) + " as ";
                String name = phrase.split(" ")[1];
                sql = sql.substring(phrase.length());
                String statement = extractInParenthesis(sql);
                if (name.endsWith("segment")) {
                    queries.addAll(extractSubQueries(statement, name));
                } else {
                    queries.add(Arrays.asList(name, statement));
                }
                sql = sql.substring(statement.length() + 2);
                if (sql.startsWith(",")) {
                    sql = sql.substring(1);
                }
                sql = sql.trim();
                if (sql.startsWith("select")) {
                    break;
                } else {
                    sql = "with " + sql;
                }
            } else {
                // not more CTE
                break;
            }
        }
        queries.add(Lists.newArrayList(FINAL, sql));
        return queries;
    }

    public static List<List<String>> extractSubQueries(String sql, String alias) {
        List<List<String>> queries = new ArrayList<>();

        boolean hasChange = true;

        while(hasChange) {
            hasChange = false;

            // find "(select ...) as alias" pattern
            int count = 0;
            while (count++ < 50) {
                if (count >= 50) {
                    throw new UnsupportedOperationException("There are more than 50 select-as patterns in the sql: " + sql);
                }
                Matcher asAliasMatcher = Pattern.compile("\\) as (?<alias>[A-Za-z0-9.]+)").matcher(sql);
                boolean found = false;
                while (asAliasMatcher.find()) {
                    String stmtAlias = asAliasMatcher.group("alias");
                    String stmt = extractReverseInParenthesis(sql.substring(0, asAliasMatcher.start() + 1));
                    if (stmt.startsWith("select")) {
                        queries.addAll(extractSubQueries(stmt, stmtAlias));
                        sql = sql.replace("(" + stmt + ") as " + stmtAlias, stmtAlias);
                        found = true;
                        hasChange = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }

            // find "? in (select ...)" pattern
            count = 0;
            while (count++ < 50) {
                if (count >= 50) {
                    throw new UnsupportedOperationException("There are more than 50 in sub-query patterns in the sql: " + sql);
                }
                Matcher inMatcher = Pattern.compile("(?<outer>[A-Za-z0-9.]+) in " +
                        "\\(select (?<inner>[A-Za-z0-9.]+)").matcher(sql);
                boolean found = false;
                while (inMatcher.find()) {
                    String outer = inMatcher.group("outer");
                    String inner = inMatcher.group("inner");
                    String stmt = extractInParenthesis(sql.substring(inMatcher.end("outer") + " in ".length()));
                    if (stmt.contains("where")) {
                        inner = inner.contains(".") ? //
                                StringUtils.substringAfterLast(inner, ".") : inner;
                        String innerAlias = TranslatorUtils.generateAlias(inner);
                        queries.addAll(extractSubQueries(stmt, innerAlias));
                        sql = sql.replace(outer + " in (" + stmt + ")", //
                                outer + " in (select " + inner + " from " + innerAlias + ")");
                        found = true;
                        hasChange = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }
        }

        queries.add(Arrays.asList(alias, sql));
        return queries;
    }

    private static String extractInParenthesis(String str) {
        StringBuilder sb = new StringBuilder();
        int numOfOpenParenthesis = 1;
        for (int i = 1; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == ')' && numOfOpenParenthesis == 1) {
                break;
            } else {
                sb.append(c);
                if (c == '(') {
                    numOfOpenParenthesis++;
                } else if (c == ')') {
                    numOfOpenParenthesis--;
                }
            }
        }
        return sb.toString();
    }

    private static String extractReverseInParenthesis(String str) {
        StringBuilder sb = new StringBuilder();
        int numOfOpenParenthesis = 1;
        str = StringUtils.reverse(str);
        for (int i = 1; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '(' && numOfOpenParenthesis == 1) {
                break;
            } else {
                sb.append(c);
                if (c == ')') {
                    numOfOpenParenthesis++;
                } else if (c == '(') {
                    numOfOpenParenthesis--;
                }
            }
        }
        return StringUtils.reverse(sb.toString());
    }

}
