package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocationUtils {

    private static final Log log = LogFactory.getLog(LocationUtils.class);

    private static Map<String, String> countrySynonMap;
    private static Map<String, String> usStateSynonMap;
    private static Map<String, String> usStateAbbrToFullNameMap;
    private static Map<String, String> usStateFullNameToAbbrMap;
    private static Map<String, String> usRegionMap;
    private static Map<String, String> caStateSynonMap;
    private static Map<String, String> caStateAbbrToFullNameMap;
    private static Map<String, String> caStateFullNameToAbbrMap;

    public static final String USA = "USA";
    public static final String CANADA = "CANADA";
    public static final String US = "US";

    static {
        Object[][] countrySynonData = new Object[][] { //
                { USA, new String[] { "USA", "U S A", "US", "U S", "UNITED STATES", "UNITEDSTATES",
                        "UNITED STATES AMERICA", "AMERICA", "AMERICAN", "UNITED STATES AMERICA USA",
                        "UNITED STATES USA", "ESTADOS UNIDOS" } }, //
                { CANADA, new String[] { "CANADA", "CA" } }, //
                { "ANTIGUA BARBUDA", new String[] { "ANTIGUA BARBUDA", "ANTIGUABARBUDA", "ANTIGUA" } }, //
                { "ASIA PACIFIC REGION", new String[] { "ASIA PACIFIC REGION", "ASIAPACIFIC REGION", "AP", "APAC" } }, //
                { "ÅLAND IS", new String[] { "ÅLAND IS", "ALAND IS" } }, //
                { "AUSTRIA", new String[] { "AUSTRIA", "ÖSTERREICH" } }, //
                { "AMERICAN SAMOA", new String[] { "AMERICAN SAMOA", "SAMOA AMERICAN" } }, //

                { "ALGERIA", new String[] { "ALGERIA", "ALGERIE", "ALGÉRIE" } }, //
                { "AUSTRALIA", new String[] { "AUSTRALIA", "COCOS KEELING IS" } }, //
                { "AFGHANISTAN", new String[] { "AFGHANISTAN", "AFGANISTÁN" } }, //
                { "BOSNIA HERZEGOVINA",
                        new String[] { "BOSNIAHERZ", "BOSNIA HERZEGOVINA", "BOSNIAHERZEGOVINA", "BOSNIA HERZ",
                                "BOSNIA" } }, //
                { "BRUNEI DARUSSALAM", new String[] { "BRUNEI DARUSSALAM", "BRUNEI" } }, //
                { "BOLIVIA", new String[] { "BOLIVIA", "BOLIVIA PLURINATIONAL STATE", "PLURINATIONAL STATE BOLIVIA" } }, //
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
                                "REPUBLIC CONGO", "REP CONGO", "DEM REP CONGO", "CONGO DEMOCRATIC REPUBLIC CONGO" } }, //
                { "CYPRUS", new String[] { "CYPRUS", "REPUBLIC CYPRUS", "CYPRUS REPUBLIC" } }, //
                { "CHINA",
                        new String[] { "CHINA", "REPUBLIC CHINA", "P R CHINA", "PEOPLES REPUBLIC CHINA",
                                "PEOPLES REP CHINA", "PR CHINA", "GREATER CHINA" } }, //
                { "CROATIA", new String[] { "CROATIA", "REPUBLIC CROATIA", "HRVATSKA", "CROATIA HRVATSKA" } }, //
                { "CÔTE D IVOIRE",
                        new String[] { "CÔTE D IVOIRE", "IVORY COAST COTE DIVOIRE", "COTE DIVOIRE", "COTE D IVOIRE",
                                "IVORY COAST", "COTE DIVOIRE IVORY COAST", "CÔTE DIVOIRE", "COTE D", "C TE DIVOIRE" } }, //
                { "CZECH REPUBLIC", new String[] { "CZECH REPUBLIC", "CZECHIA", "CZECH", "ČESKÁ REPUBLIKA" } }, //
                { "CURAÇAO", new String[] { "CURAÇAO", "CURAÃ§AO", "CURACAO" } }, //
                { "CAPE VERDE", new String[] { "CAPE VERDE", "CAPEVERDE" } }, //
                { "CAMEROON", new String[] { "CAMEROON", "CAMEROUN" } }, //
                { "CENTRAL AFRICAN REPUBLIC", new String[] { "CENTRAL AFRICAN REPUBLIC", "CENTRAL AFRICAN REP" } }, //
                { "DOMINICAN REPUBLIC",
                        new String[] { "DOMINICAN REPUBLIC", "REPUBLICA DOMINICANA", "REPÚBLICA DOMINICANA",
                                "DOMINICAN REP", "DOMINICANA REPÚBLICA" } }, //
                { "EQUATORIAL GUINEA", new String[] { "EQUATORIAL GUINEA", "REPUBLIC EQUATORIAL GUINEA" } }, //
                { "FINLAND", new String[] { "FINLAND", "SUOMI" } }, //
                { "FIJI", new String[] { "FIJI", "REPUBIC FIJI" } }, //
                { "FRANCE",
                        new String[] { "FRANCE", "NEW CALEDONIAN", "FRANKREICH", "FRANCE METROPOLITAN",
                                "FRANCE MARTINIQUE" } }, //
                { "FRENCH POLYNESIA", new String[] { "FRENCH POLYNESIA", "POLYNÉSIE FRANÇAISE", "FRENC POLYNESIA" } }, //
                { "FALKLAND IS MALVINAS", new String[] { "FALKLAND IS MALVINAS", "FALKLAND IS" } }, //
                { "GUAM", new String[] { "GUAM", "ICELAND GUAM" } }, //
                { "GABON", new String[] { "GABON", "GABON REPUPLIC" } }, //
                { "GERMANY", new String[] { "GERMANY", "DUITSLAND", "DEUTSCHLAND", "ALEMANIA" } }, //
                { "HONG KONG",
                        new String[] { "HONG KONG", "HONGKONG", "CHINA HONG KONG", "HONG KONG CHINA", "HONG KONG SAR",
                                "HONG KONG SAR CHINA" } }, //
                { "IRELAND", new String[] { "IRELAND", "REPUBLIC IRELAND", "IRELAND REPUBLIC", "EIRE" } }, //
                { "IRAN ISLAMIC REPUBLIC",
                        new String[] { "IRAN ISLAMIC REPUBLIC", "IRAN", "IRAN REPUBLIC", "ISLAMIC REPUBLIC IRAN" } }, //
                { "JORDAN", new String[] { "JORDAN", "HASHEMITE KINGDOM JORDAN" } }, //
                { "KOREA REPUBLIC",
                        new String[] { "KOREA REPUBLIC", "SOUTH KOREA", "KOREA", "S KOREA", "REPUBLIC KOREA",
                                "KOREA REPUBLIC SOUTH KOREA", "KOREA REPLUBLIC", "KOREA SOUTH", "KOREA REP" } }, //
                { "KOREA DEMOCRATIC PEOPLE S REPUBLIC",
                        new String[] { "KOREA DEMOCRATIC PEOPLE S REPUBLIC", "NORTH KOREA",
                                "KOREA DEMOCRATIC PEOPLES REPUBLIC", "KOREA DEMOCRATIC PEOPLES REP", "KOREA D P R O",
                                "KOREAREPUBLIC", "KOREA DPR", "KOREA NORTH" } }, //
                { "KYRGYZSTAN", new String[] { "KYRGYZSTAN", "KIRGHIZIA" } }, //
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
                                "MACEDONIA FORMER YUGOSLAV", "MACEDONIA", "FYRO MACEDONIA", "REPUBLIC MACEDONIA" } }, //
                { "MARSHALL IS", new String[] { "MARSHALL IS", "REPUBLIC MARSHALL IS" } }, //
                { "MOLDOVA REPUBLIC", new String[] { "MOLDOVA REPUBLIC", "REPUBLIC MOLDOVA", "MOLDOVA" } }, //
                { "MICRONESIA FEDERATED STATES",
                        new String[] { "MICRONESIA FEDERATED STATES", "MICRONESIA", "FEDERATED STATES MICRONESIA",
                                "MICRONESIA FED ST" } }, //
                { "MOROCCO", new String[] { "MOROCCO", "MAROC" } }, //
                { "MALI", new String[] { "MALI", "MALI REPUBLIQUE" } }, //
                { "MACAO", new String[] { "MACAO", "MACAU CHINA", "MACAU", "MACAO SAR" } }, //
                { "MEXICO", new String[] { "MEXICO", "MÉXICO" } }, //
                { "NORWAY", new String[] { "NORWAY", "NORGE" } }, //
                { "NETHERLANDS",
                        new String[] { "NETHERLANDS", "NETHERLANDS NE", "NETHERLANDS ANTILLES", "NETHERLANDS HOLLAND",
                                "KINGDOM NETHERLANDS", "HOLLAND", "HOLANDSKO" } }, //
                { "NEW CALEDONIA", new String[] { "NEW CALEDONIA", "NEW CALÉDONIA" } }, //
                { "NIUE", new String[] { "NIUE", "NIUE IS" } }, //
                { "OMAN", new String[] { "OMAN", "SULTANATE OMAN" } }, //
                { "PANAMA", new String[] { "PANAMA", "REPUBLICA DE PANAMA" } }, //
                { "POLAND", new String[] { "POLAND", "POLSKA" } }, //
                { "PALESTINE STATE",
                        new String[] { "PALESTINE STATE", "PALESTINIAN TERRITORY OCCUPIED", "PALESTINIAN TERRITORY",
                                "PALESTINIAN TERRITORIES", "PALESTINIAN AUTHORITY", "PALESTINE" } }, //
                { "PERU", new String[] { "PERU", "PERÚ" } }, //
                { "PAPUA NEW GUINEA", new String[] { "PAPUA NEW GUINEA", "ADMIRALTY IS" } }, //
                { "RUSSIAN FEDERATION",
                        new String[] { "RUSSIAN FEDERATION", "RUSSIA FEDERATION", "RUSSIA", "RUSSIAN FED", "RUSSIAN",
                                "RU S A SIAN FEDERATION" } }, //
                { "RÉUNION", new String[] { "RÉUNION", "FR REUNION", "REUNION" } }, //
                { "ST HELENA ASCENSION TRISTAN DA CUNHA",
                        new String[] { "ST HELENA ASCENSION TRISTAN DA CUNHA", "ASCENSION IS", "ST HELENA" } }, //
                { "SAMOA", new String[] { "SAMOA", "WESTERN SAMOA", "SAMOA WESTERN" } }, //
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
                        new String[] { "SOUTH GEORGIA SOUTH SANDWICH IS", "SOUTH GEORGIA SOUTH SANDWICH ISL",
                                "SOUTH SANDWICH IS" } }, //
                { "SLOVENIA", new String[] { "SLOVENIA", "SLOVENIJA" } }, //
                { "SLOVAKIA", new String[] { "SLOVAKIA", "SLOVAKIA SLOVAK REPUBLIC", "SLOVAK REPUBLIC" } }, //
                { "SIERRA LEONE", new String[] { "SIERRA LEONE", "SIERRA LEONE REPUBLIC" } }, //
                { "SERBIA", new String[] { "SERBIA", "SERBIA MONTENEGRO", "SERBIA REPUBLIC SERBIA" } }, //
                { "SAUDI ARABIA", new String[] { "SAUDI ARABIA", "SAUDIARABIA", "KINGDOM SAUDI ARABIA" } }, //
                { "ST BARTHÉLEMY", new String[] { "ST BARTHÉLEMY", "ST BARTHELEMY" } }, //
                { "SAO TOME PRINCIPE",
                        new String[] { "SAO TOME PRINCIPE", "STOME PRINCIPE", "SÃO TOMÉ PRÍNCIPE",
                                "S TOME PRINCIPE" } }, //
                { "SINGAPORE", new String[] { "SINGAPORE", "REPUBLIC SINGAPORE" } }, //
                { "ST VINCENT GRENADINES", new String[] { "ST VINCENT GRENADINES", "ST VINCENT" } }, //
                { "SVALBARD JAN MAYEN", new String[] { "SVALBARD JAN MAYEN", "SVALBARD" } }, //
                { "TURKEY",
                        new String[] { "TURKEY", "TURKISH", "TÜRKIYE", "TURKIYE", "TURKISH REP N CYPRUS",
                                "NORTHERN CYPRUS" } }, //
                { "TUNISIA", new String[] { "TUNISIA", "TUNISIE" } }, //
                { "TRINIDAD TOBAGO", new String[] { "TRINIDAD TOBAGO", "TRINIDAD" } }, //
                { "TANZANIA UNITED REPUBLIC",
                        new String[] { "TANZANIA UNITED REPUBLIC", "TANZANIA", "UNITED REPUBLIC TANZANIA" } }, //
                { "TAIWAN PROVINCE CHINA",
                        new String[] { "TAIWAN PROVINCE CHINA", "TAIWAN ROC", "TAIWAN REPUBLIC CHINA", "TAIWAN R O C",
                                "TAIWAN CHINA", "TAIWAN", "TAI WAN", "PROVINCE CHINA TAIWAN" } }, //
                { "TIMOR LESTE", new String[] { "TIMOR LESTE", "EAST TIMOR" } }, //
                { "TOKELAU", new String[] { "TOKELAU", "TOKELAU IS" } }, //
                { "UNITED KINGDOM",
                        new String[] { "UNITED KINGDOM", "UNITED KINGDOM GREAT BRITAIN NORTHERN IRELAND",
                                "GREAT BRITAIN", "GREAT BRITAIN UK", "NORTHERN IRELAND", "WALES",
                                "UNITED KINGDOM GREAT BRITAIN", "UK", "U K", "SCOTLAND", "ROYAUME UNI", "ENGLAND",
                                "ENG", "EN", "REINO UNIDO", "UNITED KINGDOM GB NI" } }, //
                { "UNITED STATES MINOR OUTLYING IS",
                        new String[] { "UNITED STATES MINOR OUTLYING IS", "BAKER", "BAKER IS", "HOWLAND", "HOWLAND IS",
                                "JARVIS", "JARVIS IS", "JOHNSTON ATOLL", "KINGMAN REEF", "MIDWAY", "MIDWAY IS",
                                "NAVASSA", "NAVASSA IS", "PALMYRA ATOLL", "WAKE", "WAKE IS", "US MINOR OUTLYING IS",
                                "U S MINOR IS", "US MINOR IS", "USA MINOR IS", "UNITED STATES MINOR OUTLYING" } }, //
                { "UNITED ARAB EMIRATES",
                        new String[] { "UNITED ARAB EMIRATES", "UTD ARAB EMIR", "UAE", "UNITED EMIRATES", "U A E",
                                "UTDARAB EMIR", "ARAB EMIRATES", "ABU DHABI", "AJMAN", "AL AIN", "DUBAI", "DUBAI UAE",
                                "FUJAIRAH", "RAS AL KHAIMAH", "SHARJAH", "UMM AL QUWAIN" } }, //
                { "UGANDA", new String[] { "UGANDA", "UGANDA REPUBLIC", "UDA" } }, //
                { "UKRAINE", new String[] { "UKRAINE" } }, //
                { "VIRGIN IS BRITISH",
                        new String[] { "VIRGIN IS BRITISH", "BRITISH VIRGIN IS", "CHANNEL IS", "ANEGADA",
                                "JOST VAN DYKE", "TORTOLA", "VIRGIN GORDA", "BRIT VIRGIN IS", "VIRGIN IS UK" } }, //
                { "VIRGIN IS US",
                        new String[] { "US VIRGIN IS", "U S VIRGIN IS", "VIRGIN IS US", "VIRGIN IS U S",
                                "VIRGIN IS USA", "VIRGIN IS", "U S IS VIRGIN", "USA VIRGIN IS", "ST CROIX", "ST JOHN",
                                "ST THOMAS", "AMER VIRGIN IS" } }, //
                { "VIET NAM", new String[] { "VIET NAM", "VIETNAM" } }, //
                { "VENEZUELA",
                        new String[] { "VENEZUELA", "VENEZUELA BOLIVARIAN", "VENEZUELA BOLIVARIAN REPUBLIC",
                                "BOLIVARIAN REPUBLIC VENEZUELA" } }, //
                { "WESTERN SAHARA", new String[] { "WESTERN SAHARA", "WEST SAHARA", "SAHARA" } }, //
                { "ZIMBABWE", new String[] { "ZIMBABWE", "ZIMBABWE REPUBLIC" } }, //
        };

        Object[][] usStateSynonData = new Object[][] { //
                { "AL", new String[] { "AL", "A L", "ALABAMA", "ALA" } }, //
                { "AK", new String[] { "AK", "A K", "ALASKA" } }, //
                { "AS", new String[] { "AS", "A S", "AMERICAN SAMOA", "AMERICANSAMOA" } }, //
                { "AZ", new String[] { "AZ", "A Z", "ARIZONA", "ARIZ" } }, //
                { "AR", new String[] { "AR", "A R", "ARKANSAS", "ARK" } }, //
                { "CA", new String[] { "CA", "C A", "CF", "C F", "CAL", "CALI", "CALIFORNIA", "CALIF" } }, //
                { "CO", new String[] { "CO", "C O", "CL", "C L", "COLORADO", "COLO" } }, //
                { "CT", new String[] { "CT", "C T", "CONNECTICUT", "CONN" } }, //
                { "DE", new String[] { "DE", "D E", "DL", "D L", "DELAWARE", "DEL" } }, //
                { "DC", new String[] { "DC", "D C", "DIST OF COLUMBIA", "DISTRICT OF COLUMBIA", "WASHINGTON D C",
                        "WASHINGTON DC", "WASH DC", "WASH D C", "DISTOFCOLUMBIA", "DISTRICTOFCOLUMBIA", "WASHINGTONDC",
                        "WASHDC" } }, //
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
                { "MH", new String[] { "MH", "M H", "MARSHALL ISLANDS", "MARSHALL", "MARSHALLISLANDS" } }, //
                { "MA", new String[] { "MA", "M A", "MASSACHUSETTS", "MASS" } }, //
                { "MI", new String[] { "MI", "M I", "MC", "M C", "MICHIGAN", "MICH" } }, //
                { "FM", new String[] { "FM", "F M", "MICRONESIA" } }, //
                { "MN", new String[] { "MN", "M N", "MINNESOTA", "MINN" } }, //
                { "MS", new String[] { "MS", "M S", "MISSISSIPPI", "MISS" } }, //
                { "MO", new String[] { "MO", "M O", "MISSOURI" } }, //
                { "MT", new String[] { "MT", "M T", "MONTANA", "MONT" } }, //
                { "NE", new String[] { "NE", "N E", "NB", "N B", "NEBRASKA", "NEBR", "NEB" } }, //
                { "NV", new String[] { "NV", "N V", "NEVADA", "NEV" } }, //
                { "NH", new String[] { "NH", "N H", "NEW HAMPSHIRE", "NEWHAMPSHIRE" } }, //
                { "NJ", new String[] { "NJ", "N J", "NEW JERSEY", "NEWJERSEY" } }, //
                { "NM", new String[] { "NM", "N M", "NEW MEXICO", "NEWMEXICO" } }, //
                { "NY", new String[] { "NY", "N Y", "NEW YORK", "NEWYORK" } }, //
                { "NC", new String[] { "NC", "N C", "NORTH CAROLINA", "NORTHCAROLINA" } }, //
                { "ND", new String[] { "ND", "N D", "NORTH DAKOTA", "NORTHDAKOTA" } }, //
                { "MP", new String[] { "MP", "M P", "NORTHERN MARIANAS", "MARIANAS", "NORTHERNMARIANAS" } }, //
                { "OH", new String[] { "OH", "O H", "OHIO" } }, //
                { "OK", new String[] { "OK", "O K", "OKLAHOMA", "OKLA" } }, //
                { "OR", new String[] { "OR", "O R", "OREGON", "ORE", "OREG" } }, //
                { "PW", new String[] { "PW", "P W", "PALAU" } }, //
                { "PA", new String[] { "PA", "P A", "PENNSYLVANIA" } }, //
                { "PR", new String[] { "PR", "P R", "PUERTO RICO", "PUERTORICO" } }, //
                { "RI", new String[] { "RI", "R I", "RHODE ISLAND", "RHODEISLAND" } }, //
                { "SC", new String[] { "SC", "S C", "SOUTH CAROLINA", "SOUTHCAROLINA" } }, //
                { "SD", new String[] { "SD", "S D", "SOUTH DAKOTA", "SOUTHDAKOTA" } }, //
                { "TN", new String[] { "TN", "T N", "TENNESSEE", "TENN" } }, //
                { "TX", new String[] { "TX", "T X", "TEXAS", "TEX" } }, //
                { "UT", new String[] { "UT", "U T", "UTAH" } }, //
                { "VT", new String[] { "VT", "V T", "VERMONT" } }, //
                { "VA", new String[] { "VA", "V A", "VIRGINIA" } }, //
                { "VI", new String[] { "VI", "V I", "VIRGIN ISLANDS", "VIRGINISLANDS" } }, //
                { "WA", new String[] { "WA", "W A", "WN", "W N", "WASHINGTON", "WASH" } }, //
                { "WV", new String[] { "WV", "W V", "WEST VIRGINIA", "WVA", "WESTVIRGINIA" } }, //
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
                { "DC", "Washington DC" }, //
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
                { "BC", new String[] { "BC", "B C", "BRITISH COLUMBIA", "COLOMBIE-BRITANNIQUE", "BRITISHCOLUMBIA" } }, //
                { "MB", new String[] { "MB", "M B", "MANITOBA" } }, //
                { "NB", new String[] { "NB", "N B", "NEW BRUNSWICK", "NOUVEAU-BRUNSWICK", "NEWBRUNSWICK" } }, //
                { "NL", new String[] { "NL", "N L", "NEWFOUNDLAND AND LABRADOR", "TERRE-NEUVE-ET-LABRADOR",
                        "NEWFOUNDLANDANDLABRADOR" } }, //
                { "NS", new String[] { "NS", "N S", "NOVA SCOTIA", "NOUVELLE-ÉCOSSE", "NOVASCOTIA" } }, //
                { "NT", new String[] { "NT", "N T", "NORTHWEST TERRITORIES", "TERRITOIRES DU NORD-OUEST",
                        "NORTHWESTTERRITORIES", "TERRITOIRESDUNORD-OUEST" } }, //
                { "NU", new String[] { "NU", "N U", "NUNAVUT" } }, //
                { "ON", new String[] { "ON", "O N", "ONTARIO" } }, //
                { "PE", new String[] { "PE", "P E", "PRINCE EDWARD ISLAND", "ÎLE-DU-PRINCE-ÉDOUARD",
                        "PRINCEEDWARDISLAND" } }, //
                { "QC", new String[] { "QC", "Q C", "QUEBEC", "QUéBEC", "QUÉBEC" } }, //
                { "SK", new String[] { "SK", "S K", "SASKATCHEWAN" } }, //
                { "YT", new String[] { "YT", "Y T", "YUKON", "YUKON TERRITORY", "YUKONTERRITORY" } }, //
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
                { "New England",
                        new String[] { "Connecticut", "Maine", "Massachusetts", "New Hampshire", "Rhode Island",
                                "Vermont" } }, //
                { "Mid-Atlantic", new String[] { "New Jersey", "New York", "Pennsylvania" } }, //
                { "East North Central", new String[] { "Illinois", "Indiana", "Michigan", "Ohio", "Wisconsin" } }, //
                { "West North Central",
                        new String[] { "Iowa", "Kansas", "Minnesota", "Missouri", "Nebraska", "North Dakota",
                                "South Dakota" } }, //
                { "South Atlantic",
                        new String[] { "Delaware", "Florida", "Georgia", "Maryland", "North Carolina", "South Carolina",
                                "Virginia", "Washington DC", "West Virginia" } }, //
                { "East South Central", new String[] { "Alabama", "Kentucky", "Mississippi", "Tennessee" } }, //
                { "West South Central", new String[] { "Arkansas", "Louisiana", "Oklahoma", "Texas" } }, //
                { "Mountain",
                        new String[] { "Arizona", "Colorado", "Idaho", "Montana", "Nevada", "New Mexico", "Utah",
                                "Wyoming" } }, //
                { "Pacific", new String[] { "Alaska", "California", "Hawaii", "Oregon", "Washington" } } //
        };

        countrySynonMap = dataToMap(countrySynonData, false);
        usStateSynonMap = dataToMap(usStateSynonData, false);
        usStateAbbrToFullNameMap = dataToMap(usStateAbbrData, false);
        usStateFullNameToAbbrMap = dataToMap(usStateAbbrData, true);
        usRegionMap = dataToMap(usRegionData, false);
        caStateSynonMap = dataToMap(caStateSynonData, false);
        caStateAbbrToFullNameMap = dataToMap(caStateAbbrData, false);
        caStateFullNameToAbbrMap = dataToMap(caStateAbbrData, true);
    }

    private static Map<String, String> dataToMap(Object[][] data, boolean reverse) {
        Map<String, String> toReturn = new HashMap<>();
        for (Object[] entry : data) {
            if (entry[1] instanceof String[]) {
                String value = (String) entry[0];
                for (String key : (String[]) entry[1]) {
                    toReturn.put(key.toUpperCase(), value.toUpperCase());
                }
            } else {
                if (reverse) {
                    toReturn.put(((String) entry[1]).toUpperCase(), ((String) entry[0]).toUpperCase());
                } else {
                    toReturn.put(((String) entry[0]).toUpperCase(), ((String) entry[1]).toUpperCase());
                }

            }
        }
        return toReturn;
    }

    public static String getStandardCountry(String country) {
        if (StringUtils.isEmpty(country)) {
            return null;
        }
        country = country.replaceAll("\\d", "");
        String phrase = LocationStringStandardizationUtils.getStandardString(country);
        if (StringUtils.isEmpty(phrase)) {
            return null;
        }
        phrase = phrase.replace(" ISLAND ", " IS ").replace(" ISLANDS ", " IS ").replace(" ISLND ", " IS ")
                .replace(" ISLNDS ", " IS ").replaceAll(" ISLAND$", " IS").replaceAll(" ISLANDS$", " IS")
                .replaceAll(" ISLND$", " IS").replaceAll(" ISLNDS$", " IS").replaceAll("^ISLAND ", "IS ")
                .replaceAll("^ISLANDS ", "IS ").replaceAll("^ISLND ", "IS ").replaceAll("^ISLNDS ", "IS ");
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
        standardCountry = StringUtils.isNotEmpty(standardCountry) ? standardCountry : USA;

        if (USA.equalsIgnoreCase(standardCountry)) {
            Map<String, String> stateLookUp = usStateSynonMap;
            String guess = getStateFromMap(state, stateLookUp);
            if (usStateAbbrToFullNameMap.containsKey(guess)) {
                return LocationStringStandardizationUtils.getStandardString(usStateAbbrToFullNameMap.get(guess));
            } else {
                log.info(String.format("Fail to map USA state %s to standardized state code", state));
                return null;
            }
        } else if (CANADA.equalsIgnoreCase(standardCountry)) {
            Map<String, String> stateLookUp = caStateSynonMap;
            String guess = getStateFromMap(state, stateLookUp);
            if (caStateAbbrToFullNameMap.containsKey(guess)) {
                return LocationStringStandardizationUtils.getStandardString(caStateAbbrToFullNameMap.get(guess));
            } else {
                log.info(String.format("Fail to map Canada state %s to standardized state code", state));
                return null;
            }
        } else {
            return LocationStringStandardizationUtils.getStandardString(state);
        }
    }

    public static String getStardardStateCode(String standardCountry, String standardState) {
        if (USA.equalsIgnoreCase(standardCountry) && usStateFullNameToAbbrMap.containsKey(standardState)) {
            return usStateFullNameToAbbrMap.get(standardState);
        } else if (CANADA.equalsIgnoreCase(standardCountry) && caStateFullNameToAbbrMap.containsKey(standardState)) {
            return caStateFullNameToAbbrMap.get(standardState);
        } else {
            return standardState;
        }
    }

    public static String getStandardRegion(String standardCountry, String standardState) {
        if (USA.equalsIgnoreCase(standardCountry)) {
            if (StringUtils.isNotEmpty(standardState) && usRegionMap.containsKey(standardState.toUpperCase())) {
                return LocationStringStandardizationUtils
                        .getStandardString(usRegionMap.get(standardState.toUpperCase()));
            } else {
                return "OTHER";
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
            if (guess != null) {
                return guess;
            }
            nTerms--;
        }
        return null;
    }

    private static String[] getNTerms(String[] tokens, int n) {
        if (tokens.length >= n) {
            String[] terms = new String[tokens.length - n + 1];
            for (int i = 0; i < tokens.length - n + 1; i++) {
                String[] termTokens = new String[n];
                System.arraycopy(tokens, i, termTokens, 0, n);
                terms[i] = StringUtils.join(termTokens, " ");
            }
            return terms;
        } else {
            return new String[] {};
        }
    }

    private static String getStandardStateFromPhrases(String[] phrases, Map<String, String> stateLookUp) {
        List<String> phraseList = new ArrayList<>(Arrays.asList(phrases));
        Collections.reverse(phraseList);
        for (String phrase : phraseList) {
            String guess = getStandardStateFromPhrase(phrase, stateLookUp);
            if (guess != null) {
                return guess;
            }
        }
        return null;
    }

    private static String getStandardStateFromPhrase(String phrase, Map<String, String> stateLookUp) {
        // phrase = phrase.trim();
        phrase = phrase.replaceAll(" ", "");
        if (stateLookUp.containsKey(phrase)) {
            return stateLookUp.get(phrase);
        } else {
            return null;
        }
    }

}