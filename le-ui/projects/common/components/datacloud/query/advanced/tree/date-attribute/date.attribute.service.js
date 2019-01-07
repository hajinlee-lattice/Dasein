import './date-attribute-edit.scss';

angular.module('common.datacloud.query.builder.tree.edit.date.attribute')
    .service('QueryTreeDateAttributeStore', function (QueryTreeDateAttributeService) {
        var QueryTreeDateAttributeStore = this;
        QueryTreeDateAttributeStore.periods = [];

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
                { 'name': 'LAST', 'displayName': 'Last' },
                { 'name': 'BETWEEN', 'displayName': 'Between Last' },
                { 'name': 'BETWEEN_DATE', 'displayName': 'Between' },
                { 'name': 'BEFORE', 'displayName': 'Before' },
                { 'name': 'AFTER', 'displayName': 'After' },
                { 'name': 'IS_EMPTY', 'displayName': 'Is Empty' }
            ];
        }

        this.periodList = function () {
            if (QueryTreeDateAttributeStore.periods.length == 0) {
                // ["Week","Month","Quarter","Year"]
                QueryTreeDateAttributeStore.periods.push({ 'name': 'Day', 'displayName': 'Day(s)' });
                // QueryTreeDateAttributeStore.periods.push({ 'name': 'Week', 'displayName': 'Week(s)' });
                // QueryTreeDateAttributeStore.periods.push({ 'name': 'Week', 'displayName': 'Week(s)' });
                // QueryTreeDateAttributeStore.periods.push({ 'name': 'Week', 'displayName': 'Week(s)' });
                // QueryTreeDateAttributeService.getPeriods().then(
                //     function (result) {
                //         result.forEach(function (element) {
                //             QueryTreeDateAttributeStore.periods.push({ 'name': element, 'displayName': element+'(s)' });
                //         });

                //     }
                // );
            }
            return QueryTreeDateAttributeStore.periods;
        }
        
    })
    .service('QueryTreeDateAttributeService', function ($http, $q) {


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