package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;


public class LocationUtils {

    private static Map<String, String> countrySynonMap;
    private static Map<String, String> usStateSynonMap;
    private static Map<String, String> usStateAbbrMap;
    private static Map<String, String> usRegionMap;
    private static Map<String, String> caStateSynonMap;
    private static Map<String, String> caStateAbbrMap;

    public static final String USA = "USA";

    static {
        Object[][] countrySynonData = new Object[][]{ //
                { USA, new String[] { "USA", "U S A", "US", "U S", "United States", "United States of America", "America" } }, //
        };

        Object[][] usStateSynonData = new Object[][] { //
                { "AL", new String[] { "AL", "A L", "ALABAMA", "ALA" } }, //
                { "AK", new String[] { "AK", "A K", "ALASKA" } }, //
                { "AS", new String[] { "AS", "A S", "AMERICAN SAMOA" } }, //
                { "AZ", new String[] { "AZ", "A Z", "ARIZONA", "ARIZ" } }, //
                { "AR", new String[] { "AR", "A R", "ARKANSAS", "ARK" } }, //
                { "CA", new String[] { "CA", "C A", "CF", "C F", "CAL", "CALI", "CALIFORNIA", "CALIF" } }, //
                { "CO", new String[] { "CO", "C O", "CL", "C L", "COLORADO", "COLO" } }, //
                { "CT", new String[] { "CT", "C T", "CONNECTICUT", "CONN" } }, //
                { "DE", new String[] { "DE", "D E", "DL", "D L", "DELAWARE", "DEL" } }, //
                { "DC", new String[] { "DC", "D C", "DIST OF COLUMBIA", "DISTRICT OF COLUMBIA", "WASHINGTON D C",
                        "WASHINGTON DC", "WASH DC", "WASH D C" } }, //
                { "FL", new String[] { "FL", "F L", "FLORIDA", "FLA" } }, //
                { "GA", new String[] { "GA", "G A", "GEORGIA" } }, //
                { "GU", new String[] { "GU", "G U", "GUAM" } }, //
                { "HI", new String[] { "HI", "H I", "HA", "H A", "HAWAII" } }, //
                { "ID", new String[] { "ID", "I D", "IDAHO" } }, //
                { "IL", new String[] { "IL", "I L", "ILLINOIS", "ILL" } }, //
                { "IN", new String[] { "IN", "I N", "INDIANA", "IND" } }, //
                { "IA", new String[] { "IA", "I A", "IOWA" } }, //
                { "KS", new String[] { "KS", "K S", "KA", "K A", "KAN", "KANSAS", "KANS" } }, //
                { "KY", new String[] { "KY", "K Y", "KENTUCKY" } }, //
                { "LA", new String[] { "LA", "L A", "LOUISIANA" } }, //
                { "ME", new String[] { "ME", "M E", "MAINE" } }, //
                { "MD", new String[] { "MD", "M D", "MARYLAND" } }, //
                { "MH", new String[] { "MH", "M H", "MARSHALL ISLANDS", "MARSHALL" } }, //
                { "MA", new String[] { "MA", "M A", "MASSACHUSETTS", "MASS" } }, //
                { "MI", new String[] { "MI", "M I", "MC", "M C", "MICHIGAN", "MICH" } }, //
                { "FM", new String[] { "FM", "F M", "MICRONESIA" } }, //
                { "MN", new String[] { "MN", "M N", "MINNESOTA", "MINN" } }, //
                { "MS", new String[] { "MS", "M S", "MISSISSIPPI", "MISS" } }, //
                { "MO", new String[] { "MO", "M O", "MISSOURI" } }, //
                { "MT", new String[] { "MT", "M T", "MONTANA", "MONT" } }, //
                { "NE", new String[] { "NE", "N E", "NB", "N B", "NEBRASKA", "NEBR", "NEB" } }, //
                { "NV", new String[] { "NV", "N V", "NEVADA", "NEV" } }, //
                { "NH", new String[] { "NH", "N H", "NEW HAMPSHIRE" } }, //
                { "NJ", new String[] { "NJ", "N J", "NEW JERSEY" } }, //
                { "NM", new String[] { "NM", "N M", "NEW MEXICO" } }, //
                { "NY", new String[] { "NY", "N Y", "NEW YORK" } }, //
                { "NC", new String[] { "NC", "N C", "NORTH CAROLINA" } }, //
                { "ND", new String[] { "ND", "N D", "NORTH DAKOTA" } }, //
                { "MP", new String[] { "MP", "M P", "NORTHERN MARIANAS", "MARIANAS" } }, //
                { "OH", new String[] { "OH", "O H", "OHIO" } }, //
                { "OK", new String[] { "OK", "O K", "OKLAHOMA", "OKLA" } }, //
                { "OR", new String[] { "OR", "O R", "OREGON", "ORE", "OREG" } }, //
                { "PW", new String[] { "PW", "P W", "PALAU" } }, //
                { "PA", new String[] { "PA", "P A", "PENNSYLVANIA" } }, //
                { "PR", new String[] { "PR", "P R", "PUERTO RICO" } }, //
                { "RI", new String[] { "RI", "R I", "RHODE ISLAND" } }, //
                { "SC", new String[] { "SC", "S C", "SOUTH CAROLINA" } }, //
                { "SD", new String[] { "SD", "S D", "SOUTH DAKOTA" } }, //
                { "TN", new String[] { "TN", "T N", "TENNESSEE", "TENN" } }, //
                { "TX", new String[] { "TX", "T X", "TEXAS", "TEX" } }, //
                { "UT", new String[] { "UT", "U T", "UTAH" } }, //
                { "VT", new String[] { "VT", "V T", "VERMONT" } }, //
                { "VA", new String[] { "VA", "V A", "VIRGINIA" } }, //
                { "VI", new String[] { "VI", "V I", "VIRGIN ISLANDS" } }, //
                { "WA", new String[] { "WA", "W A", "WN", "W N", "WASHINGTON", "WASH" } }, //
                { "WV", new String[] { "WV", "W V", "WEST VIRGINIA", "WVA" } }, //
                { "WI", new String[] { "WI", "W I", "WS", "W S", "WISCONSIN", "WIS", "WISC" } }, //
                { "WY", new String[] { "WY", "W Y", "WYOMING", "WYO" } } //
        };

        String[][] usStateAbbrData = new String[][] { //
                { "AL", "Alabama" }, //
                { "AK", "Alaska" }, //
                { "AS", "American Samoa" }, //
                { "AZ", "Arizona" }, //
                { "AR", "Arkansas" }, //
                { "CA", "California" }, //
                { "CO", "Colorado" }, //
                { "CT", "Connecticut" }, //
                { "DE", "Delaware" }, //
                { "DC", "Washington D.C." }, //
                { "FL", "Florida" }, //
                { "GA", "Georgia" }, //
                { "GU", "Guam" }, //
                { "HI", "Hawaii" }, //
                { "ID", "Idaho" }, //
                { "IL", "Illinois" }, //
                { "IN", "Indiana" }, //
                { "IA", "Iowa" }, //
                { "KS", "Kansas" }, //
                { "KY", "Kentucky" }, //
                { "LA", "Louisiana" }, //
                { "ME", "Maine" }, //
                { "MD", "Maryland" }, //
                { "MH", "Marshall Islands" }, //
                { "MA", "Massachusetts" }, //
                { "MI", "Michigan" }, //
                { "FM", "Micronesia" }, //
                { "MN", "Minnesota" }, //
                { "MS", "Mississippi" }, //
                { "MO", "Missouri" }, //
                { "MT", "Montana" }, //
                { "NE", "Nebraska" }, //
                { "NV", "Nevada" }, //
                { "NH", "New Hampshire" }, //
                { "NJ", "New Jersey" }, //
                { "NM", "New Mexico" }, //
                { "NY", "New York" }, //
                { "NC", "North Carolina" }, //
                { "ND", "North Dakota" }, //
                { "MP", "Northern Marianas" }, //
                { "OH", "Ohio" }, //
                { "OK", "Oklahoma" }, //
                { "OR", "Oregon" }, //
                { "PW", "Palau" }, //
                { "PA", "Pennsylvania" }, //
                { "PR", "Puerto Rico" }, //
                { "RI", "Rhode Island" }, //
                { "SC", "South Carolina" }, //
                { "SD", "South Dakota" }, //
                { "TN", "Tennessee" }, //
                { "TX", "Texas" }, //
                { "UT", "Utah" }, //
                { "VT", "Vermont" }, //
                { "VA", "Virginia" }, //
                { "VI", "Virgin Islands" }, //
                { "WA", "Washington" }, //
                { "WV", "West Virginia" }, //
                { "WI", "Wisconsin" }, //
                { "WY", "Wyoming" } };

        Object[][] caStateSynonData = new Object[][] { //
                { "AB", new String[] { "AB", "A B", "ALBERTA" } }, //
                { "BC", new String[] { "BC", "B C", "BRITISH COLUMBIA", "COLOMBIE-BRITANNIQUE" } }, //
                { "MB", new String[] { "MB", "M B", "MANITOBA" } }, //
                { "NB", new String[] { "NB", "N B", "NEW BRUNSWICK", "NOUVEAU-BRUNSWICK" } }, //
                { "NL", new String[] { "NL", "N L", "NEWFOUNDLAND AND LABRADOR", "TERRE-NEUVE-ET-LABRADOR" } }, //
                { "NS", new String[] { "NS", "N S", "NOVA SCOTIA", "NOUVELLE-ÉCOSSE" } }, //
                { "NT", new String[] { "NT", "N T", "NORTHWEST TERRITORIES", "TERRITOIRES DU NORD-OUEST" } }, //
                { "NU", new String[] { "NU", "N U", "NUNAVUT" } }, //
                { "ON", new String[] { "ON", "O N", "ONTARIO" } }, //
                { "PE", new String[] { "PE", "P E", "PRINCE EDWARD ISLAND", "ÎLE-DU-PRINCE-ÉDOUARD" } }, //
                { "QC", new String[] { "QC", "Q C", "QUEBEC", "QUéBEC", "QUÉBEC" } }, //
                { "SK", new String[] { "SK", "S K", "SASKATCHEWAN" } }, //
                { "YT", new String[] { "YT", "Y T", "YUKON", "YUKON TERRITORY" } }, //
        };

        String[][] caStateAbbrData = new String[][] { //
                { "AB", "Alberta" }, //
                { "BC", "British Columbia" }, //
                { "MB", "Manitoba" }, //
                { "NB", "New Brunswick" }, //
                { "NL", "Newfoundland and Labrador" }, //
                { "NS", "Nova Scotia" }, //
                { "NT", "Northwest Territories" }, //
                { "NU", "Nunavut" }, //
                { "ON", "Ontario" }, //
                { "PE", "Prince Edward Island" }, //
                { "QC", "Quebec" }, //
                { "SK", "Saskatchewan" }, //
                { "YT", "Yukon Territory" } //
        };

        Object[][] usRegionData = new Object[][] { //
                { "New England", new String[]{ "Connecticut", "Maine", "Massachusetts", "New Hampshire", "Rhode Island", "Vermont" } },
                { "Mid-Atlantic", new String[]{ "New Jersey", "New York", "Pennsylvania" } },
                { "East North Central", new String[]{ "Illinois", "Indiana", "Michigan", "Ohio", "Wisconsin" } },
                { "West North Central", new String[]{ "Iowa", "Kansas", "Minnesota", "Missouri", "Nebraska", "North Dakota", "South Dakota" } },
                { "South Atlantic", new String[]{ "Delaware", "Florida", "Georgia", "Maryland", "North Carolina", "South Carolina", "Virginia", "Washington D.C.", "West Virginia" } },
                { "East South Central", new String[]{ "Alabama", "Kentucky", "Mississippi", "Tennessee" } },
                { "West South Central", new String[]{ "Arkansas", "Louisiana", "Oklahoma", "Texas" } },
                { "Mountain", new String[]{ "Arizona", "Colorado", "Idaho", "Montana", "Nevada", "New Mexico", "Utah", "Wyoming" } },
                { "Pacific", new String[]{ "Alaska", "California", "Hawaii", "Oregon", "Washington" } }
        };

        countrySynonMap = dataToMap(countrySynonData);
        usStateSynonMap = dataToMap(usStateSynonData);
        usStateAbbrMap = dataToMap(usStateAbbrData);
        usRegionMap = dataToMap(usRegionData);
        caStateSynonMap = dataToMap(caStateSynonData);
        caStateAbbrMap = dataToMap(caStateAbbrData);
    }

    private static Map<String, String> dataToMap(Object[][] data) {
        Map<String, String> toReturn = new HashMap<>();
        for (Object[] entry: data) {
            if (entry[1] instanceof String[]) {
                String value = (String) entry[0];
                for (String key : (String[]) entry[1]) {
                    toReturn.put(key.toUpperCase(), value);
                }
            } else {
                toReturn.put((String) entry[0], (String) entry[1]);
            }
        }
        return toReturn;
    }

    public static String getStandardCountry(String country) {
        String phrase = country.toUpperCase().replace(".", "").trim();
        if (countrySynonMap.containsKey(phrase)) {
            return countrySynonMap.get(phrase);
        } else {
            return country;
        }
    }

    public static String getStandardState(String standardCountry, String state) {
        if (StringUtils.isEmpty(state)) {
            return null;
        }

        if (USA.equalsIgnoreCase(standardCountry)) {
            Map<String, String> stateLookUp = usStateSynonMap;
            String guess = getStateFromMap(state, stateLookUp);
            if (usStateAbbrMap.containsKey(guess)) {
                return usStateAbbrMap.get(guess);
            } else {
                return null;
            }
        } else if ("Canada".equalsIgnoreCase(standardCountry)) {
            Map<String, String> stateLookUp = caStateSynonMap;
            String guess = getStateFromMap(state, stateLookUp);
            if (caStateAbbrMap.containsKey(guess)) {
                return caStateAbbrMap.get(guess);
            } else {
                return null;
            }
        } else {
            return state;
        }
    }

    public static String getStandardRegion(String standardCountry, String standardState) {
        if (USA.equalsIgnoreCase(standardCountry)) {
            if (StringUtils.isNotEmpty(standardState) && usRegionMap.containsKey(standardState.toUpperCase())) {
                return usRegionMap.get(standardState.toUpperCase());
            } else {
                return "Other";
            }
        } else {
            return standardCountry;
        }
    }

    private static String getStateFromMap(String state, Map<String, String> stateLookUp) {
        String[] phrases = state.toUpperCase().replace(".", "").split("[^\\p{L}\\s.]");
        int nTerms = phrases.length;
        while (nTerms > 0) {
            String[] terms = getNTerms(phrases, nTerms);
            String guess = getStandardStateFromPhrases(terms, stateLookUp);
            if (guess != null)
            {
                return guess;
            }
            nTerms--;
        }
        return null;
    }

    private static String[] getNTerms(String[] tokens, int n)
    {
        if (tokens.length >= n)
        {
            String[] terms = new String[tokens.length - n + 1];
            for (int i = 0; i < tokens.length - n + 1; i++)
            {
                String[] termTokens = new String[n];
                System.arraycopy(tokens, i, termTokens, 0, n);
                terms[i] = StringUtils.join(termTokens, " ");
            }
            return terms;
        }
        else
        {
            return new String[]{};
        }
    }

    private static String getStandardStateFromPhrases(String[] phrases, Map<String, String> stateLookUp)
    {
        List<String> phraseList = new ArrayList<>(Arrays.asList(phrases));
        Collections.reverse(phraseList);
        for (String phrase : phraseList)
        {
            String guess = getStandardStateFromPhrase(phrase, stateLookUp);
            if (guess != null)
            {
                return guess;
            }
        }
        return null;
    }

    private static String getStandardStateFromPhrase(String phrase, Map<String, String> stateLookUp)
    {
        phrase = phrase.trim();
        if (stateLookUp.containsKey(phrase))
        {
            return stateLookUp.get(phrase);
        }
        else
        {
            return null;
        }
    }

}
