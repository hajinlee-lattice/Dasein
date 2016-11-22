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
                { USA, new String[] { "USA", "U S A", "US", "U S", "UNITED STATES", "UNITEDSTATES",
                        "UNITED STATES AMERICA", "AMERICA", "AMERICAN", "UNITED STATES AMERICA USA",
                        "UNITED STATES USA" } }, //
                { "ANTIGUA BARBUDA", new String[] { "ANTIGUA BARBUDA", "ANTIGUABARBUDA", "ANTIGUA" } }, //
                { "ASIA PACIFIC REGION", new String[] { "ASIA PACIFIC REGION", "ASIAPACIFIC REGION", "AP", "APAC" } }, //
                { "ÅLAND IS", new String[] { "ÅLAND IS", "ALAND IS" } }, //
                { "AUSTRIA", new String[] { "AUSTRIA", "ÖSTERREICH" } }, //
                { "AMERICAN SAMOA", new String[] { "AMERICAN SAMOA", "SAMOA AMERICAN" } }, //

                { "ALGERIA", new String[] { "ALGERIA", "ALGERIE", "ALGÉRIE" } }, //
                { "BOSNIA HERZEGOVINA",
                        new String[] { "BOSNIAHERZ", "BOSNIA HERZEGOVINA", "BOSNIAHERZEGOVINA", "BOSNIA HERZ",
                                "BOSNIA" } }, //
                { "BRUNEI DARUSSALAM", new String[] { "BRUNEI DARUSSALAM", "BRUNEI" } }, //
                { "BOLIVIA", new String[] { "BOLIVIA", "BOLIVIA PLURINATIONAL STATE" } }, //
                { "BELARUS", new String[] { "BELARUS", "REPUBLIC BELARUS" } }, //
                { "BAHRAIN", new String[] { "BAHRAIN", "KINGDOM OF BAHRAIN" } }, //
                { "BURKINA FASO", new String[] { "BURKINA FASO", "BURKINAFASO" } }, //
                { "BOTSWANA", new String[] { "BOTSWANA", "BOTSWANA REPUBLIC" } }, //
                { "BONAIRE SINT EUSTATIUS SABA",
                        new String[] { "BONAIRE SINT EUSTATIUS SABA", "BONAIRE ST EUSTATIUS SABA" } }, //
                { "BELGIUM", new String[] { "BELGIUM", "BELGIË", "BELGIE" } }, //
                { "CONGO",
                        new String[] { "CONGO", "CONGO KINSHASA", "CONGO BRAZZAVILLE", "CONGO DEMOCRATIC REPUBLIC",
                                "CONGO DRC", "CONGO DEMOCRATIC REP", "DEMOCRATIC REPUBLIC CONGO", "RÉPUBLIQUE DU CONGO",
                                "REPUBLIC CONGO", "REPOF CONGO", "DEM REP CONGO", "CONGO DEMOCRATIC REPUBLIC CONGO" } }, //
                { "CYPRUS", new String[] { "CYPRUS", "REPUBLIC CYPRUS", "CYPRUS REPUBLIC" } }, //
                { "CHINA",
                        new String[] { "CHINA", "REPUBLIC CHINA", "PR CHINA", "PEOPLES REPUBLIC CHINA",
                                "PEOPLES REP CHINA", "PR CHINA", "GREATER CHINA" } }, //
                { "CROATIA", new String[] { "CROATIA", "REPUBLIC CROATIA", "HRVATSKA", "CROATIA HRVATSKA" } }, //
                { "CÔTE D IVOIRE",
                        new String[] { "CÔTE D IVOIRE", "IVORY COAST COTE DIVOIRE", "COTE DIVOIRE", "COTE D IVOIRE",
                                "IVORY COAST", "COTE DIVOIRE IVORY COAST", "CÔTE DIVOIRE", "COTE D", "C TE DIVOIRE" } }, //
                { "CZECH REPUBLIC", new String[] { "CZECH REPUBLIC", "CZECHIA", "CZECH", "ČESKÁ REPUBLIKA" } }, //
                { "CURAÇAO", new String[] { "CURAÇAO", "CURAÃ§AO", "CURACAO" } }, //
                { "CAPE VERDE", new String[] { "CAPE VERDE", "CAPEVERDE" } }, //
                { "CAMEROON", new String[] { "CAMEROON", "CAMEROUN" } }, //
                { "DOMINICAN REPUBLIC",
                        new String[] { "DOMINICAN REPUBLIC", "REPUBLICA DOMINICANA", "REPÚBLICA DOMINICANA",
                                "DOMINICAN REP" } }, //
                { "EQUATORIAL GUINEA", new String[] { "EQUATORIAL GUINEA", "REPUBLIC EQUATORIAL GUINEA" } }, //
                { "FINLAND", new String[] { "FINLAND", "SUOMI" } }, //
                { "FIJI", new String[] { "FIJI", "REPUBIC FIJI" } }, //
                { "FRANCE",
                        new String[] { "FRANCE", "NEW CALEDONIAN", "FRANKREICH", "FRANCE METROPOLITAN",
                                "FRANCE MARTINIQUE" } }, //
                { "FRENCH POLYNESIA", new String[] { "FRENCH POLYNESIA", "POLYNÉSIE FRANÇAISE", "FRENCPOLYNESIA" } }, //
                { "FALKLAND IS MALVINAS", new String[] { "FALKLAND IS MALVINAS", "FALKLAND IS" } }, //
                { "GUAM", new String[] { "GUAM", "ICELAND GUAM" } }, //
                { "GABON", new String[] { "GABON", "GABON REPUPLIC" } }, //
                { "GERMANY", new String[] { "GERMANY", "DUITSLAND", "DEUTSCHLAND" } }, //
                { "HONG KONG",
                        new String[] { "HONG KONG", "HONGKONG", "CHINA HONG KONG", "HONG KONG CHINA", "HONG KONG SAR",
                                "HONG KONG SAR CHINA" } }, //
                { "IRELAND", new String[] { "IRELAND", "REPUBLIC IRELAND", "IRELAND REPUBLIC", "EIRE" } }, //
                { "IRAN ISLAMIC REPUBLIC", new String[] { "IRAN ISLAMIC REPUBLIC", "IRAN", "IRAN REPUBLIC" } }, //
                { "JORDAN", new String[] { "JORDAN", "HASHEMITE KINGDOM JORDAN" } }, //
                { "KOREA REPUBLIC",
                        new String[] { "KOREA REPUBLIC", "SOUTH KOREA", "KOREA", "S KOREA", "REPUBLIC KOREA",
                                "KOREA REPUBLIC SOUTH KOREA", "KOREA REPLUBLIC", "KOREA SOUTH" } }, //
                { "KOREA DEMOCRATIC PEOPLE S REPUBLIC",
                        new String[] { "KOREA DEMOCRATIC PEOPLE S REPUBLIC", "NORTH KOREA",
                                "KOREA DEMOCRATIC PEOPLES REPUBLIC", "KOREA DEMOCRATIC PEOPLES REP", "KOREA DPRO",
                                "KOREAREPUBLIC", "KOREA DPR", "KOREA NORTH" } }, //
                { "LUXEMBOURG",
                        new String[] { "LUXEMBOURG", "LUXEMBURG", "GRAND DUCHY LUXEMBOURG", "DUCHY LUXEMBOURG" } }, //
                { "LIBYA", new String[] { "LIBYA", "LIBYANARABJAMAHIRIYA", "LIBYAN ARAB JAMAHIRIYA" } }, //
                { "LATVIA", new String[] { "LATVIA", "LATVIJA" } }, //
                { "LAO PEOPLE S DEMOCRATIC REPUBLIC",
                        new String[] { "LAO PEOPLE S DEMOCRATIC REPUBLIC", "LAOS", "LAO PRD",
                                "LAO PEOPLES DEMOCRATIC REPUBLIC" } }, //
                { "MYANMAR", new String[] { "MYANMAR", "BURMA MYANMAR", "MYANMAR BURMA", "BURMA" } }, //
                { "MACEDONIA FORMER YUGOSLAV REPUBLIC",
                        new String[] { "MACEDONIA FORMER YUGOSLAV REPUBLIC", "YUGOSLAV", "YUGOSLAVIA",
                                "MACEDONIA FORMER YUGOSLAV", "MACEDONIA", "FYRO MACEDONIA" } }, //
                { "MARSHALL IS", new String[] { "MARSHALL IS", "REPUBLIC MARSHALL IS" } }, //
                { "MOLDOVA REPUBLIC", new String[] { "MOLDOVA REPUBLIC", "REPUBLIC MOLDOVA", "MOLDOVA" } }, //
                { "MICRONESIA FEDERATED STATES",
                        new String[] { "MICRONESIA FEDERATED STATES", "MICRONESIA", "FEDERATED STATES MICRONESIA" } }, //
                { "MOROCCO", new String[] { "MOROCCO", "MAROC" } }, //
                { "MALI", new String[] { "MALI", "MALI REPUBLIQUE" } }, //
                { "MACAO", new String[] { "MACAO", "MACAU CHINA", "MACAU", "MACAO SAR" } }, //
                { "MEXICO", new String[] { "MEXICO", "MÉXICO" } }, //
                { "NORWAY", new String[] { "NORWAY", "NORGE" } }, //
                { "NETHERLANDS",
                        new String[] { "NETHERLANDS", "NETHERLANDS NE", "NETHERLANDS ANTILLES", "NETHERLANDS HOLLAND",
                                "KINGDOM NETHERLANDS", "HOLLAND", "HOLANDSKO" } }, //
                { "NEW CALEDONIA", new String[] { "NEW CALEDONIA", "NEW CALÉDONIA" } }, //
                { "OMAN", new String[] { "OMAN", "SULTANATE OMAN" } }, //
                { "PANAMA", new String[] { "PANAMA", "REPUBLICA DE PANAMA" } }, //
                { "POLAND", new String[] { "POLAND", "POLSKA" } }, //
                { "PALESTINE STATE",
                        new String[] { "PALESTINE STATE", "PALESTINIAN TERRITORY OCCUPIED", "PALESTINIAN TERRITORY",
                                "PALESTINIAN TERRITORIES", "PALESTINIAN AUTHORITY", "PALESTINE" } }, //
                { "PERU", new String[] { "PERU", "PERÚ" } }, //
                { "RUSSIAN FEDERATION",
                        new String[] { "RUSSIAN FEDERATION", "RUSSIA FEDERATION", "RUSSIA", "RUSSIAN FED",
                                "RUSSIAN" } }, //
                { "RÉUNION", new String[] { "RÉUNION", "FR REUNION", "REUNION" } }, //
                { "ST HELENA ASCENSION TRISTAN DA CUNHA",
                        new String[] { "ST HELENA ASCENSION TRISTAN DA CUNHA", "ASCENSION IS", "ST HELENA" } }, //
                { "SAMOA", new String[] { "SAMOA", "WESTERN SAMOA" } }, //
                { "SYRIAN ARAB REPUBLIC",
                        new String[] { "SYRIAN ARAB REPUBLIC", "SYRIAN ARAB REPUBLIC SYRIA", "SYRIA" } }, //
                { "SWITZERLAND", new String[] { "SWITZERLAND", "SVIZZERA", "SUISSE", "SCHWEIZ" } }, //
                { "SWEDEN", new String[] { "SWEDEN", "SVERIGE", "SCHWEDEN" } }, //
                { "SOUTH AFRICA", new String[] { "SOUTH AFRICA", "SÜDAFRIKA" } }, //
                { "SINT MAARTEN DUTCH PART", new String[] { "SINT MAARTEN DUTCH PART", "ST MAARTEN", "SINT MAARTEN" } }, //
                { "ST MARTIN FRENCH PART", new String[] { "ST MARTIN FRENCH PART", "ST MARTIN" } }, //
                { "ST KITTS NEVIS", new String[] { "ST KITTS NEVIS", "ST KITTS" } }, //
                { "SRI LANKA", new String[] { "SRI LANKA", "SRILANKA" } }, //
                { "SERBIA", new String[] { "SERBIA", "SRBIJA", "REPUBLIC KOSOVO", "REP KOSOVO", "KOSOVO" } }, //
                { "SPAIN", new String[] { "SPAIN", "SPANISH", "SPANIEN", "ESPAÑA", "ESPANA" } }, //
                { "SOUTH GEORGIA SOUTH SANDWICH IS",
                        new String[] { "SOUTH GEORGIA SOUTH SANDWICH IS", "SOUTH GEORGIA SOUTH SANDWICH ISL" } }, //
                { "SLOVENIA", new String[] { "SLOVENIA", "SLOVENIJA" } }, //
                { "SLOVAKIA", new String[] { "SLOVAKIA", "SLOVAKIA SLOVAK REPUBLIC", "SLOVAK REPUBLIC" } }, //
                { "SIERRA LEONE", new String[] { "SIERRA LEONE", "SIERRA LEONE REPUBLIC" } }, //
                { "SERBIA", new String[] { "SERBIA", "SERBIA MONTENEGRO", "SERBIA REPUBLIC SERBIA" } }, //
                { "SAUDI ARABIA", new String[] { "SAUDI ARABIA", "SAUDIARABIA", "KINGDOM SAUDI ARABIA" } }, //
                { "ST BARTHÉLEMY", new String[] { "ST BARTHÉLEMY", "ST BARTHELEMY" } }, //
                { "SAO TOME PRINCIPE", new String[] { "SAO TOME PRINCIPE", "STOME PRINCIPE", "SÃO TOMÉ PRÍNCIPE" } }, //
                { "SINGAPORE", new String[] { "SINGAPORE", "REPUBLIC SINGAPORE" } }, //
                { "TURKEY", new String[] { "TURKEY", "TURKISH", "TÜRKIYE", "TURKIYE" } }, //
                { "TUNISIA", new String[] { "TUNISIA", "TUNISIE" } }, //
                { "TRINIDAD TOBAGO", new String[] { "TRINIDAD TOBAGO", "TRINIDAD" } }, //
                { "TANZANIA UNITED REPUBLIC", new String[] { "TANZANIA UNITED REPUBLIC", "TANZANIA" } }, //
                { "TAIWAN PROVINCE CHINA",
                        new String[] { "TAIWAN PROVINCE CHINA", "TAIWAN ROC", "TAIWAN REPUBLIC CHINA", "TAIWAN R O C",
                                "TAIWAN CHINA", "TAIWAN", "TAI WAN", "PROVINCE CHINA TAIWAN" } }, //
                { "TIMOR LESTE", new String[] { "TIMOR LESTE", "EAST TIMOR" } }, //
                { "UNITED KINGDOM",
                        new String[] { "UNITED KINGDOM", "UNITED KINGDOM GREAT BRITAIN NORTHERN IRELAND",
                                "GREAT BRITAIN", "GREAT BRITAIN UK", "NORTHERN IRELAND", "WALES",
                                "UNITED KINGDOM GREAT BRITAIN", "UK", "U K", "SCOTLAND", "ROYAUME UNI", "ENGLAND",
                                "ENG", "EN" } }, //
                { "UNITED STATES MINOR OUTLYING IS",
                        new String[] { "UNITED STATES MINOR OUTLYING IS", "BAKER", "BAKER IS", "HOWLAND", "HOWLAND IS",
                                "JARVIS", "JARVIS IS", "JOHNSTON ATOLL", "KINGMAN REEF", "MIDWAY", "MIDWAY IS",
                                "NAVASSA", "NAVASSA IS", "PALMYRA ATOLL", "WAKE", "WAKE IS", "US MINOR OUTLYING IS",
                                "U S MINOR IS", "US MINOR IS", "USA MINOR IS" } }, //
                { "UNITED ARAB EMIRATES",
                        new String[] { "UNITED ARAB EMIRATES", "UTD ARAB EMIR", "UAE", "UNITED EMIRATES", "U A E",
                                "UTDARAB EMIR", "ARAB EMIRATES" } }, //
                { "UGANDA", new String[] { "UGANDA", "UGANDA REPUBLIC", "UDA" } }, //
                { "VIRGIN IS BRITISH",
                        new String[] { "VIRGIN IS BRITISH", "BRITISH VIRGIN IS", "CHANNEL IS", "ANEGADA",
                                "JOST VAN DYKE", "TORTOLA", "VIRGIN GORDA", "BRITVIRGIN IS" } }, //
                { "VIRGIN IS US",
                        new String[] { "US VIRGIN IS", "U S VIRGIN IS", "VIRGIN IS US", "VIRGIN IS U S",
                                "VIRGIN IS USA", "VIRGIN IS", "U S IS VIRGIN", "USA VIRGIN IS", "ST CROIX", "ST JOHN",
                                "ST THOMAS", "AMERVIRGIN IS" } }, //
                { "VIET NAM", new String[] { "VIET NAM", "VIETNAM" } }, //
                { "VENEZUELA", new String[] { "VENEZUELA", "VENEZUELA BOLIVARIAN", "VENEZUELA BOLIVARIAN REPUBLIC" } }, //
                { "WESTERN SAHARA", new String[] { "WESTERN SAHARA", "WEST SAHARA", "SAHARA" } }, //
                { "ZIMBABWE", new String[] { "ZIMBABWE", "ZIMBABWE REPUBLIC" } }, //
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
        country = country.replaceAll("\\d", "");
        String phrase = com.latticeengines.common.exposed.util.StringUtils.getStandardString(country);
        phrase = phrase.replace(" ISLAND ", " IS ").replace(" ISLANDS ", " IS ").replaceAll(" ISLAND$", " IS")
                .replaceAll(" ISLANDS$", " IS").replaceAll("^ISLAND ", "IS ").replaceAll("^ISLANDS ", "IS ");
        phrase = phrase.replace(" SAINT ", " ST ").replaceAll(" SAINT$", " ST").replaceAll("^SAINT ", "ST ");
        phrase = phrase.replace(" OF ", " ").replaceAll(" OF$", "").replaceAll("^OF ", "");
        phrase = phrase.replace(" AND ", " ");
        phrase = phrase.replace(" THE ", " ").replaceAll(" THE$", "").replaceAll("^THE ", "");
        if (countrySynonMap.containsKey(phrase)) {
            return countrySynonMap.get(phrase);
        } else {
            return phrase;
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