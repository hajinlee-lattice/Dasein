angular.module('mainApp.appCommon.utilities.NumberUtility', [])
.service('NumberUtility', function () {
   
    /**
     * [AbbreviateLargeNumber Given a number, will return the abbreviationiated version (e.g. 14000 becomes 14k)]
     * @param {[type]} number         an integer
     * @param {[type]} decimal_places an integer
     * return: a string
     */
    this.AbbreviateLargeNumber = function (number, decimal_places) {
        if (number == null || typeof number != "number") {
            return null;
        }
        
        decimal_places = decimal_places != null && typeof decimal_places === 'number' ? decimal_places : 2;
        decimal_places = Math.pow(10,decimal_places);

        var abbreviation = ["K", "M", "B", "T"];
    
        for (var i = abbreviation.length-1; i>=0; i--) {
            var size = Math.pow(10,(i+1)*3);

            if(size <= number) {
                 number = Math.round(number*decimal_places/size)/decimal_places;
    
                 if((number == 1000) && (i < abbreviation.length - 1)) {
                     number = 1;
                     i++;
                 }
    
                 number += abbreviation[i];
    
                 break;
            }
        }
        return number;
    };

});