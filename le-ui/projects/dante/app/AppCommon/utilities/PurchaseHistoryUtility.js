angular
    .module('mainApp.appCommon.utilities.PurchaseHistoryUtility', [
        'mainApp.appCommon.utilities.NumberUtility',
        'mainApp.appCommon.utilities.ResourceUtility'
    ])
    .service('PurchaseHistoryUtility', function(
        NumberUtility,
        ResourceUtility
    ) {
        var PERIOD_ENUMS = {
            M: [
                'Jan',
                'Feb',
                'Mar',
                'Apr',
                'May',
                'Jun',
                'Jul',
                'Aug',
                'Sep',
                'Oct',
                'Nov',
                'Dec'
            ],
            Q: ['1', '2', '3', '4']
        };

        // period to momentjs format map
        this.periodToMomentFormat = {
            D: 'YYYY MMM DD',
            M: 'YYYY MMM',
            Q: 'YYYY Q',
            Y: 'YYYY'
        };

        this.formatNumber = function(number, abbreviate) {
            number = Math.ceil(number);

            if (abbreviate === true) {
                if (number >= 1000000) {
                    return NumberUtility.AbbreviateLargeNumber(number, 2);
                } else {
                    return NumberUtility.AbbreviateLargeNumber(number, 1);
                }
            } else {
                return NumberUtility.NumberWithCommas(number);
            }
        };

        this.formatDollar = function(number, abbreviate) {
            var currencySymbol = ResourceUtility.getString('CURRENCY_SYMBOL');
            var sign = number < 0 ? '- ' : '';

            return (
                sign +
                currencySymbol +
                this.formatNumber(Math.abs(number), abbreviate)
            );
        };

        this.formatPercent = function(number) {
            if (typeof number !== 'number') {
                return null;
            }

            number = Math.round(number * 10) / 10;

            var toReturn = number === 0 ? 0 : number.toFixed(1);
            return toReturn + '%';
        };

        this.periodIdComparator = function(a, b) {
            // even though Quarter (1-4) converts (Jan-Apr), its still ascending order
            return new Date(a) - new Date(b);
        };

        this.getPrevYearPeriod = function(period) {
            // period format should always begin with YYYY and a space
            if (!period) {
                return;
            }
            var part = period.split(' ');
            part[0] = parseInt(part[0]) - 1;
            return isNaN(part[0]) ? period : part.join(' ');
        };

        /*
         * DantePurchaseHistory.PurchaseHistoryAttributes.PeriodOffSet property is time since DantePurchaseHistory.PeriodStartDate (epoch time in s, not ms) in the same units as DantePurchaseHistory.Period
         * eg Period = 'M', PeriodStartDate = 1461792101, PeriodOffset = 3: is 1461792101 + 3 months
         * converts this to momentjs object
         */
        this.convertPeriodOffsetToDate = function(
            period,
            periodStartDate,
            periodOffset
        ) {
            var key;
            switch (period) {
                case 'D':
                    key = 'days';
                    break;
                case 'M':
                    key = 'months';
                    break;
                case 'Q':
                    key = 'quarters';
                    break;
                case 'Y':
                    key = 'years';
                    break;
                default:
                    key = 'days';
                    break;
            }

            return moment.utc(periodStartDate * 1000).add(periodOffset, key);
        };

        this.isValidPeriodId = function(period, periodId) {
            if (!period || !periodId) {
                return false;
            }

            var parts = periodId.split(' ');
            var year = parseInt(parts[0]);

            var periodEnums = PERIOD_ENUMS[period];
            if (!periodEnums) {
                return false;
            }

            if (
                !year ||
                year > new Date().getFullYear() ||
                periodEnums.indexOf(parts[1]) === -1
            ) {
                return false;
            }

            return true;
        };

        this.formatDisplayPeriodId = function(period, periodId, long) {
            var periodIdParts = periodId.split(' ');
            var formatted = periodIdParts[1];
            formatted += ' ';
            if (long) {
                formatted += periodIdParts[0];
            } else {
                formatted += "'" + periodIdParts[0].substring(2, 4);
            }

            switch (period) {
                case 'M':
                    return formatted;
                case 'Q':
                    return 'Q' + formatted;
            }
        };

        this.getAdjacentPeriodId = function(period, periodId, backwards) {
            var periodIdParts = periodId.split(' ');
            var year = parseInt(periodIdParts[0]);
            if (!year) {
                return null;
            }

            var periodEnums = PERIOD_ENUMS[period];
            if (!periodEnums) {
                return null;
            }

            var index = periodEnums.indexOf(periodIdParts[1]);
            var dir = backwards ? -1 : 1;
            var wrapIndex = backwards ? periodEnums.length - 1 : 0;
            var edgeIndex = backwards ? 0 : periodEnums.length - 1;

            if (index === edgeIndex) {
                return year + dir + ' ' + periodEnums[wrapIndex];
            }

            if (index > -1) {
                return year + ' ' + periodEnums[index + dir];
            }

            return null;
        };

        this.getNextPeriodId = function(period, periodId) {
            return this.getAdjacentPeriodId(period, periodId, false);
        };

        this.getPrevPeriodId = function(period, periodId) {
            return this.getAdjacentPeriodId(period, periodId, true);
        };

        this.getPeriodRange = function(period, startPeriodId, endPeriodId) {
            var self = this;

            if (
                !period ||
                !self.isValidPeriodId(period, startPeriodId) ||
                !self.isValidPeriodId(period, endPeriodId)
            ) {
                return [];
            }

            if (self.periodIdComparator(startPeriodId, endPeriodId) > 0) {
                return [];
            }

            var periodEnums = PERIOD_ENUMS[period];
            if (!periodEnums) {
                return [];
            }

            var periodIdParts = startPeriodId.split(' ');
            var year = parseInt(periodIdParts[0]);

            var index = PERIOD_ENUMS[period].indexOf(periodIdParts[1]);
            if (index === -1) {
                return [];
            }

            var range = [];
            var size = periodEnums.length;
            var nextPeriodId = null;
            while (nextPeriodId !== endPeriodId) {
                nextPeriodId = year + ' ' + periodEnums[index];
                range.push(nextPeriodId);
                if (index++ === size - 1) {
                    index = 0;
                    year++;
                }
            }

            return range;
        };

        /*
         * returns the periodId of the quarter the period is in
         */
        this.getQuarterFromPeriodId = function(period, periodId) {
            var self = this;
            switch (period) {
                case 'M':
                    // set date to 1 (for firefox Date object)
                    return moment(new Date(periodId + ' 1')).format(
                        self.periodToMomentFormat.Q
                    );
                case 'Q':
                    return periodId;
            }
        };

        /*
         * return the previous quarter periodId
         */
        this.getPrevQuarterPeriod = function(period, periodId) {
            var self = this;
            var periodParts = periodId.split(' ');
            var year = parseInt(periodParts[0]);
            var quarter;
            switch (period) {
                case 'M':
                    quarter = self.getQuarterFromPeriodId(period, periodId);
                    return self.getPrevQuarterPeriod('Q', quarter);
                case 'Q':
                    quarter = parseInt(periodParts[1]);
                    if (quarter && year) {
                        var prevQuarter = quarter > 1 ? quarter - 1 : 4;
                        year = prevQuarter === 4 ? year - 1 : year;

                        return year + ' ' + prevQuarter;
                    }
                    return;
            }
        };
    });
