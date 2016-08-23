angular.module('mainApp.appCommon.utilities.TimestampIntervalUtility', [])
.service('TimestampIntervalUtility', function() {
    this.getDays = function(timestamp) {
        if (!timestamp) {
            return false;
        }

        var MILLISECOND_PER_DAY = 24 * 60 * 60 * 1000;
        var numDaysAgoPasswordLastModified = Math.floor((Date.now() - timestamp) / MILLISECOND_PER_DAY);

        return numDaysAgoPasswordLastModified;
    };
    
    this.isTimestampFartherThanNinetyDaysAgo = function(timestamp) {
        return this.getDays(timestamp) >= 90;
    };
});