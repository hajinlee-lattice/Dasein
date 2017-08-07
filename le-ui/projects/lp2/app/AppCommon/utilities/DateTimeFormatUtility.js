angular.module('mainApp.appCommon.utilities.DateTimeFormatUtility', [])
.service('DateTimeFormatUtility', function () {
   
    this.OneDayInMS = 86400000;
    /*
    * Formats date according to specified format type..
    * @param {jsonDate} date in JSON format "/Date(XXXXXXXXXX-XXXX)/".
    * @param {formatType} format to find in the list of formats.
    */
    this.FormatJsonDateCSharpFormat = function (jsonDate, formatType) {
        var datetime = this.ParseJsonDate(jsonDate);
        var dtformat = this.ConvertCSharpFormat(formatType);
        if (datetime == null || isNaN(datetime) || dtformat == null) {
            return "";
        }

        return dateFormat(new Date(datetime), dtformat);
    };

    /*
    * Formats date according to specified format type..
    * @param {jsonDate} date in JSON format "/Date(XXXXXXXXXX)/".
    * @param {formatType} format to find in the list of formats.
    */
    this.FormatCSharpDate = function (jsonDate, formatType) {
        var datetime = this.ConvertCSharpDateTimeOffsetToJSDate(jsonDate);
        var dtformat = this.ConvertCSharpFormat(formatType);
        if (datetime == null || isNaN(datetime) || dtformat == null) {
            return "";
        }

        return dateFormat(new Date(datetime), dtformat);
    };
    
    this.ConvertCSharpDateTimeOffsetToJSDate = function (csDate) {
        if (csDate == null || csDate.DateTime == null || csDate.OffsetMinutes == null) {
            return null;
        }
        //DateTime will look like "/Date(XXXXXXXXXX)/"
        var csDateTime = csDate.DateTime;
        var startParenIndex = csDateTime.indexOf("(");
        var endParenIndex = csDateTime.indexOf(")");
        if (startParenIndex === -1 || endParenIndex === -1) {
            return null;
        }

        var dateTimeInt = parseInt(csDateTime.substring(startParenIndex+1, endParenIndex));
        if (dateTimeInt == null || isNaN(dateTimeInt)) {
            return null;
        }
        
        return new Date(dateTimeInt);
    };
    
    this.CalculateDaysBetweenDates = function (startDate, endDate) {
        if (startDate == null || endDate == null) {
            return null;
        }
        
        var timeInMS = endDate.getTime() - startDate.getTime();
        
        return Math.floor(timeInMS / this.OneDayInMS);
    };

    /*
    * Extract and return date in milliseconds.
    * @param {jsonDate} date in JSON format "/Date(XXXXXXXXXX-XXXX)/".
    */
    this.ParseJsonDate = function (jsonDate) {
        // extract date portion
        var txt = $.trim(jsonDate);
        if (txt.length === 0) {
            return null;
        }
        var start = txt.indexOf("(");
        var end = txt.indexOf("-");
        if (start == -1 || end == -1 || start >= end) {
            return null;
        }
        var dt = txt.substring(start + 1, end);

        return Number(dt);
    };

    /*
     * Converts a JavaScript Date to the DateTimeOffset format that C# expects,
     * i.e.: for sending datetimeoffsets from the front end to the back end.
     * @param {jsDate}: Javascript Date object
     */
    this.ConvertJSDateToCSharpDateTimeOffset = function (jsDate, isEndDate) {
        if (!(jsDate instanceof Date)) {
            return jsDate;
        }

        // For an End Date, set its time to absolute latest
        // so that comparisons will also account for things on that day (DEF-5695)
        if (isEndDate === true) {
            jsDate.setHours(23, 59, 59, 999);
        } else if (isEndDate === false) { // Start Date - set its time to absolute earliest
            jsDate.setHours(0, 0, 0, 0);
        }

        var utcMsec = jsDate.getTime(); // milliseconds since 1970/01/01
        var cSharpDateTimeOffset = {
            DateTime: '/Date(' + utcMsec + ')/',
            OffsetMinutes : -1 * jsDate.getTimezoneOffset() // js does utc minus local, so need to negate
        };
        return cSharpDateTimeOffset;
    };

    /*
     * Convert a simple Date representation from C# (e.g. {Year:2013, Month:9, Day:1})
     * to a JavaScript Date object.
     */
    this.ConvertCSharpSimpleDateToJSDate = function (simpleDate) {
        var jsDate = new Date(
            simpleDate.Year,
            simpleDate.Month - 1, // JavaScript months go from 0-11
            simpleDate.Day
        );
        return jsDate;
    };

    /*
     * Converts minutes into an object with hours and minutes
     */
    this.MinutesToHoursAndMinutes = function (minutes) {
        if (isNaN(minutes)) {
            return null;
        }

        var toReturn = {};
        toReturn.Hours = parseInt(minutes / 60);
        toReturn.Minutes = (minutes % 60);
        return toReturn;
    };

    /*
    * Extract datetime format and converts it to the JavaScript format expected by date.format.js.
    * @param {formatType} format to find in the list of formats.
    */
    //TODO:pierce Whoever is using this needs to change the paramaters to not require BrowserStorage
    this.ConvertCSharpFormat = function (formatType, dateTimeFormatList) {

        // Bard has no getAppModelDoc, so return some default
        if (dateTimeFormatList == null) {
            return "m/d/yyyy h:MM tt Z";
        }
        
        var dateFormatValue = null;
        for (var i = 0; i < dateFormatList.length; i++) {
            if (dateFormatList[i].Key == formatType) {
                dateFormatValue = dateFormatList[i].Value;
                break;
            }
        }
        if (dateFormatValue == null || dateFormatValue.length === 0) {
            return null;
        }
        // replace all lower case 'm' with upper case 'M' to convert minutes format;
        // replace all 'M' to 'm' for month conversion;
        // replace 'z','zz','zzz' with single 'o'
        // replace 'K' with 'Z'
        var str = dateFormatValue.split('');

        for (i = 0; i < str.length; i++) {
            switch (str[i]) {
                case 'M': str[i] = 'm'; break;
                case 'm': str[i] = 'M'; break;
                case 'z':
                    str[i] = 'o';
                    // see if more than one z
                    for (var j = i + 1; j < str.length; j++) {
                        if (str[j] == 'z') {
                            str[j] = '';
                        }
                    }
                    break;
                case 'K': str[i] = 'Z'; break;
                default: break;
            }

        }
        dateFormatValue = str.join('');
        return dateFormatValue;
    };


    /* return right now in the format requested*/

    this.Now = function (formatType) {
    
        var dtformat = this.ConvertCSharpFormat(formatType);
        if(dtformat == null) {
            return "";
        }

        return dateFormat(new Date(), dtformat);
    };
    
    this.FormatStringDate = function(dateString, includeTime) {
        includeTime = includeTime != null && typeof includeTime === "boolean" ? includeTime : false;
        if (dateString == null || dateString === "") {
            return "";
        }
        
        var dateObj = new Date(dateString);
        if (dateObj == "Invalid Date") {
            return "";
        }
        
        if (includeTime) {
            return dateObj.toLocaleDateString() + " " + dateObj.toLocaleTimeString();
        } else {
            return dateObj.toLocaleDateString();
        }
    };
    
    this.FormatEpochDate = function (dateString) {
        if (dateString == null || dateString === "") {
            return "";
        }
        
        if(isNaN(dateString)) {
            return ""; 
        }
        
        var epochTime = parseInt(dateString) * 1000;
        var dateObj = new Date();
        dateObj.setTime(epochTime);
        
        return dateObj.toUTCString();
    };
    
    this.FormatShortDate = function (dateString) {
        return dateFormat(new Date(dateString), "m/d/yyyy");
    };

    this.FormatDateTime = function (dateString, formatString) {
        return dateFormat(new Date(dateString), formatString);
    };
});