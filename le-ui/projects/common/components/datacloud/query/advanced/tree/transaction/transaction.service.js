angular.module('common.datacloud.query.builder.tree.transaction.service', [])
    .service('QueryTreeTransactionStore', function ($q, $http, QueryTreeTransactionService) {
        var QueryTreeTransactionStore = this;
        QueryTreeTransactionStore.periods = [];
        this.getAmtConfig = function () {
            return {
                from: { name: 'from-amt', value: undefined, position: 0, type: 'Amt', min: '0', max: '', pattern:'\\\d+' },
                to: { name: 'to-amt', value: undefined, position: 1, type: 'Amt', min: '0', max: '', pattern:'\\\d+' }
            };
        }

        this.getQtyConfig = function () {
            return {
                from: { name: 'from-qty', value: undefined, position: 0, type: 'Qty', min: '0', pattern:'\\\d+' },
                to: { name: 'to-qty', value: undefined, position: 1, type: 'Qty', min: '0', pattern:'\\\d+' }
            };
        }

        this.getPeriodNumericalConfig = function () {
            return {
                from: { name: 'from-period', value: undefined, position: 0, type: 'Time', min: '1', max: '', pattern:'\\\d*' },
                to: { name: 'to-period', value: undefined, position: 1, type: 'Time', min: '1', max: '', pattern:'\\\d*' }
            };
        }

        this.getPeriodTimeConfig = function () {
            return {
                from: { name: 'from-time', initial: undefined, position: 0, type: 'Time', visible: true, pattern:'\\\d+' },
                to: { name: 'to-time', initial: undefined, position: 1, type: 'Time', visible: true, pattern:'\\\d+' }
            };
        }


        this.getCmpsList = function () {

            return [
                { 'name': 'EVER', 'displayName': 'Ever' },
                { 'name': 'IN_CURRENT_PERIOD', 'displayName': 'Current' },
                { 'name': 'WITHIN', 'displayName': 'Previous' },
                { 'name': 'PRIOR_ONLY', 'displayName': 'Only Prior to Last' },
                { 'name': 'BETWEEN', 'displayName': 'Between Last' },
                { 'name': 'BETWEEN_DATE', 'displayName': 'Between' },
                { 'name': 'BEFORE', 'displayName': 'Before' },
                { 'name': 'AFTER', 'displayName': 'After' }
            ];
        }

        this.periodList = function () {
            if (QueryTreeTransactionStore.periods.length == 0) {
                QueryTreeTransactionService.getPeriods().then(
                    function (result) {
                        result.forEach(function (element) {
                            var val = 7;
                            if(element !== 'Week'){
                                val = 1;
                            }
                            QueryTreeTransactionStore.periods.push({ 'name': element, 'displayName': element+'(s)', val: val});
                        });

                    }
                );
            }
            return QueryTreeTransactionStore.periods;
        }
        this.unitPurchasedCmpChoises = function () {
            return [
                { 'name': 'ANY', 'displayName': 'Any' },
                { 'name': 'GREATER_THAN', 'displayName': 'Greater than' },
                { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater than or Equal to' },
                { 'name': 'LESS_THAN', 'displayName': 'Less than' },
                { 'name': 'LESS_OR_EQUAL', 'displayName': 'Less than or Equal to' },
                { 'name': 'GTE_AND_LT', 'displayName': 'Between' }
            ];
        }

        this.amountSpentCmpChoises = function () {
            return [
                { 'name': 'ANY', 'displayName': 'Any' },
                { 'name': 'GREATER_THAN', 'displayName': 'Greater than' },
                { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater than or Equal to' },
                { 'name': 'LESS_THAN', 'displayName': 'Less than' },
                { 'name': 'LESS_OR_EQUAL', 'displayName': 'Less than or Equal to' },
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