angular.module('common.utilities.number', [])
    .service('NumberUtility', function () {

        /**
         * [AbbreviateLargeNumber Given a number, will return the abbreviationiated version (e.g. 14000 becomes 14k)]
         * @param {[type]} number         an integer
         * @param {[type]} decimal_places an integer
         * return: a string
         */
        this.AbbreviateLargeNumber = function (number, decimal_places, abbreviations) {
            if (number == null || typeof number != "number") {
                return null;
            }

            decimal_places = decimal_places != null && typeof decimal_places === 'number' ? decimal_places : 2;
            decimal_places = Math.pow(10, decimal_places);

            var abbreviation = abbreviations || ["K", "M", "B", "T"];

            for (var i = abbreviation.length - 1; i >= 0; i--) {
                var size = Math.pow(10, (i + 1) * 3);

                if (size <= number) {
                    number = Math.round(number * decimal_places / size) / decimal_places;

                    if ((number == 1000) && (i < abbreviation.length - 1)) {
                        number = 1;
                        i++;
                    }

                    number += abbreviation[i];

                    break;
                }
            }
            return number;
        };

        /**
         * RoundNumber Given a number and decimal point, will round to the number to that specific decimal 
         * e.g. (1.258, 2) = 1.26, (1258, 2) = 1258.00, (1258, -2) = 1200
         * @param {[type]} number         an integer
         * @param {[type]} decimal_places an integer
         * return: a string
         */
        this.RoundNumber = function (number, decimal_places) {
            if (number == null || typeof number != "number") {
                return number;
            }
            decimal_places = decimal_places != null && typeof decimal_places === 'number' ? decimal_places : 2;

            // Shift
            number = number.toString().split('e');
            number = Math.round(+(number[0] + 'e' + (number[1] ? (+number[1] + decimal_places) : decimal_places)));

            // Shift back
            number = number.toString().split('e');
            return +(number[0] + 'e' + (number[1] ? (+number[1] - decimal_places) : -decimal_places));
        };

        /**
         * make a percentage form a total
         * MakePercentage(10, 100, '%', 0) = 10%
         * MakePercentage(10, 100, null, 2) = 10.00
         * ...
         */

        this.MakePercentage = function (number, total, suffix, limit) {
            var suffix = suffix || '',
                percentage = 0;

            if (number && total) {
                percentage = ((number / total) * 100);

                if (typeof limit != 'undefined') {
                    percentage = percentage.toFixed(limit);
                }

                return percentage + suffix;
            }

            return 0;
        };

        this.PadNumber = function (number, width, charcter) {
            var charcter = charcter || '0',
                number = number + '';

            return number.length >= width ? number : new Array(width - number.length + 1).join(charcter) + number;
        }
    });