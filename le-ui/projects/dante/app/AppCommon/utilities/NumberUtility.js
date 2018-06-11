angular.module('mainApp.appCommon.utilities.NumberUtility', [])
.service('NumberUtility', function () {
   
    /*
        Purpose:        Given a number, will return the abbreviated version (e.g. 14000 becomes 14k)
        Parameters:     an integer
        Returns:        A string
    */
    this.AbbreviateLargeNumber = function (number, decPlaces) {
        if (number == null || typeof number != "number") {
            return null;
        }
        
        decPlaces = decPlaces != null && typeof decPlaces === 'number' ? decPlaces : 2;
        
        //Method acquired from: http://stackoverflow.com/questions/2685911/is-there-a-way-to-round-numbers-into-a-reader-friendly-format-e-g-1-1k
        
        // 2 decimal places => 100, 3 => 1000, etc
        decPlaces = Math.pow(10,decPlaces);

        // Enumerate number abbreviations
        var abbrev = [ "K", "M", "B", "T" ];
    
        // Go through the array backwards, so we do the largest first
        for (var i=abbrev.length-1; i>=0; i--) {
    
            // Convert array index to "1000", "1000000", etc
            var size = Math.pow(10,(i+1)*3);
    
            // If the number is bigger or equal do the abbreviation
            if(size <= number) {
                 // Here, we multiply by decPlaces, round, and then divide by decPlaces.
                 // This gives us nice rounding to a particular decimal place.
                 number = Math.round(number*decPlaces/size)/decPlaces;
    
                 // Handle special case where we round up to the next abbreviation
                 if((number == 1000) && (i < abbrev.length - 1)) {
                     number = 1;
                     i++;
                 }
    
                 // Add the letter for the abbreviation
                 number += abbrev[i];
    
                 // We are done... stop
                 break;
            }
        }
    
        return number;
    };
    
    /*
        Purpose:        Given a number, will return the comma formated version (e.g. 123456789.0 becomes 123,456,789.0)
        Parameters:     an integer or string representation of integer
        Returns:        A string
    */
    this.NumberWithCommas = function (num) {
        //http://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
        if (isNaN(num)) {
            return null;
        }

        var parts = num.toString().split(".");
        return parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",") + (parts[1] ? "." + parts[1] : "");
    };
});