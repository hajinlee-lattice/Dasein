angular.module('common.datacloud.query.builder.tree.transaction.service', [])
    .service('QueryTreeTransactionStore', function ($q, $http, QueryTreeTransactionService) {
        var QueryTreeTransactionStore = this;
        QueryTreeTransactionStore.periods = [];
        this.getAmtConfig = function () {
            return {
                from: { name: 'from-amt', value: undefined, position: 0, type: 'Amt', min: '1', max: '' },
                to: { name: 'to-amt', value: undefined, position: 1, type: 'Amt', min: '1', max: '' }
            };
        }

        this.getQtyConfig = function () {
            return {
                from: { name: 'from-qty', value: undefined, position: 0, type: 'Qty' },
                to: { name: 'to-qty', value: undefined, position: 1, type: 'Qty' }
            };
        }

        this.getPeriodNumericalConfig = function () {
            return {
                from: { name: 'from-period', value: undefined, position: 0, type: 'Time', min: '1', max: '' },
                to: { name: 'to-period', value: undefined, position: 1, type: 'Time', min: '1', max: '' }
            };
        }

        this.getPeriodTimeConfig = function () {
            return {
                from: { name: 'from-time', initial: undefined, position: 0, type: 'Time', visible: true },
                to: { name: 'to-time', initial: undefined, position: 1, type: 'Time', visible: true }
            };
        }


        this.getCmpsList = function () {

            return [
                { 'name': 'EVER', 'displayName': 'Ever' },
                { 'name': 'IN_CURRENT', 'displayName': 'Current' },
                { 'name': 'WITHIN', 'displayName': 'Previous' },
                { 'name': 'PRIOR_ONLY', 'displayName': 'Only Prior to Last' },
                { 'name': 'BETWEEN_LT', 'displayName': 'Between Last' },
                { 'name': 'BETWEEN', 'displayName': 'Between' },
                { 'name': 'BEFORE', 'displayName': 'Before' },
                { 'name': 'AFTER', 'displayName': 'After' }
            ];
        }

        this.periodList = function () {
            if (QueryTreeTransactionStore.periods.length == 0) {
                QueryTreeTransactionService.getPeriods().then(
                    function (result) {
                        result.forEach(function (element) {
                            QueryTreeTransactionStore.periods.push({ 'name': element, 'displayName': element });
                        });

                    }
                );
            }
            return QueryTreeTransactionStore.periods;
            // List that will be supported by the back end
            // return [
            //     { 'name': 'Week', 'displayName': 'Week' },
            //     { 'name': 'Month', 'displayName': 'Month' },
            //     { 'name': 'Quarter', 'displayName': 'Quarter' },
            //     { 'name': 'Year', 'displayName': 'Year' }
            // ];
        }
        this.unitPurchasedCmpChoises = function () {
            return [
                { 'name': 'ANY', 'displayName': 'Any' },
                { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                { 'name': 'LESS_OR_EQUAL', 'displayName': 'Less or Equal' },
                { 'name': 'GTE_AND_LT', 'displayName': 'Between' }
            ];
        }

        this.amountSpentCmpChoises = function () {
            return [
                { 'name': 'ANY', 'displayName': 'Any' },
                { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                { 'name': 'LESS_THAN', 'displayName': 'Less or Equal' },
                { 'name': 'GTE_AND_LT', 'displayName': 'Between' }

            ];
        }
    })
    .service('QueryTreeTransactionService', function ($http, $q) {


        this.getPeriods = function (resourceType, query) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/datacollection/periods/names'
            }).success(function (result) {
                deferred.resolve(result);
            }).error(function (result) {
                deferred.resolve(result);
            });

            return deferred.promise;
        };
    });