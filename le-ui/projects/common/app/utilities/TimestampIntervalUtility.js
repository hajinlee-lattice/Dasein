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
    this.getDateNinetyDaysAway = function(timestamp) {
        if (!timestamp) {
             return false;
        }
        var MILLISECOND_PER_DAY = 24 * 60 * 60 * 1000;
        var dateTimeNinetyDaysAway = timestamp + MILLISECOND_PER_DAY * 90;
        return dateTimeNinetyDaysAway;
    }
    this.isTimestampFartherThanNinetyDaysAgo = function(timestamp) {
        return this.getDays(timestamp) >= 90;
    };
    this.timeAgo = function(to_timestamp, from_timestamp, in_seconds){
        var to = (in_seconds ? new Date(to_timestamp*1000) : new Date(to_timestamp)),
            from = (from_timestamp ? (in_seconds ? new Date(to_timestamp*1000) : new Date(to_timestamp)) : new Date()),
            diff = (from - to) / 1000,
            units = [
                { name: "second", limit: 60, in_seconds: 1 },
                { name: "minute", limit: 3600, in_seconds: 60 },
                { name: "hour", limit: 86400, in_seconds: 3600  },
                { name: "day", limit: 604800, in_seconds: 86400 },
                { name: "week", limit: 2629743, in_seconds: 604800  },
                { name: "month", limit: 31556926, in_seconds: 2629743 },
                { name: "year", limit: null, in_seconds: 31556926 }
            ];

        if (diff < 5) {
            return "now";
        }

        var i = 0, unit;
        while (unit = units[i++]) {
            if (diff < unit.limit || !unit.limit){
                var diff =  Math.floor(diff / unit.in_seconds);
                return diff + " " + unit.name + (diff>1 ? "s" : "") + " ago";
            }
        };
    }
});