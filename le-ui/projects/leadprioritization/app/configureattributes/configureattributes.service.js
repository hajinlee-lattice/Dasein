angular.module('lp.configureattributes')
.service('ConfigureAttributesStore', function($q, $state, ConfigureAttributesService){
    var ConfigureAttributesStore = this;

    this.init = function() {
        this.purchaseHistory = null;
        this.steps = null;
        this.options = null;
    }

    this.init();

    this.clear = function() {
        this.init();
    }

    this.getPurchaseHistory = function() {
        var deferred = $q.defer();

        if(this.purchaseHistory) {
            deferred.resolve(this.purchaseHistory);
        } else {
            ConfigureAttributesService.getPurchaseHistory().then(function(result) {
                ConfigureAttributesStore.purchaseHistory = result;
                deferred.resolve(result);
            });
        }
        
        return deferred.promise;
    }

    this.getSteps = function(data, steps) {
        if(this.steps) {
            return this.steps;
        } else {
            var _data = {},
                steps = steps || {};
            for(var i in data) {
                var item = data[i];
                _data[item.metrics] = _data[item.metrics] || [];
                _data[item.metrics].push(item);
            }
            for(var key in steps) {
                var types = steps[key].type.split(','),
                    typesObj = {};
                for(var j in types) {
                    var type = types[j];
                    steps[key].data = steps[key].data || {};
                    if(_data[type]) {
                        steps[key].data[type] = _data[type];
                    }
                }
            }
            return steps;
        }
    }

    this.setOptions = function(options) {
        this.options = this.options || {};
        for(var i in options) {
            var key = i,
                option = options[key];

            this.options[key] = options[key];
        }
    }

    this.getOptions = function() {
        return this.options;
    }

    addSpendOvertimeSaveObject = function(metric, _option) {
        var periods = [];
        for(var i in _option) {
            for(var j in _option[i]) {
                var cmp = j,
                    option = _option[i][cmp],
                    period = {
                        Cmp: cmp,
                        Vals: [option.Val],
                        Period: option.Period
                    };
                periods.push(period);
            }
            var timestamp = + new Date(),
            obj = {
                metrics: metric,
                periods: periods,
                type: "PurchaseHistory",
                created: timestamp,
                updated: timestamp,
                eol: false,
                IsEOL: false
            };
            ConfigureAttributesStore.purchaseHistory.push(obj);
        }
    }

    var addDefaultSaveObject = function(metric, _option, addVals) {
        var periods = [];
        for(var j in _option) {
            var cmp = j,
                option = _option[cmp],
                period = {
                    Cmp: cmp,
                    Vals: [option.Val],
                    Period: option.Period
                };
            if(addVals) {
                if(!periods.length) {
                    period.Vals[0] = period.Vals[0];
                } else {
                    period.Vals = [periods[0].Vals[0] + 1, periods[0].Vals[0] + period.Vals[0]]; // val1 + 1, val1 + val2
                }
            }
            periods.push(period);

        }
        var timestamp = + new Date(),
        obj = {
            metrics: metric,
            periods: periods,
            type: "PurchaseHistory",
            created: timestamp,
            updated: timestamp,
            eol: false,
            IsEOL: false
        };
        ConfigureAttributesStore.purchaseHistory.push(obj);
    }

    var cleanPurchaseHistory = function() {
        var _purchaseHistory = [];
        ConfigureAttributesStore.purchaseHistory.forEach(function(item, key) {
            if(item) {
                _purchaseHistory.push(item);
            }
        });
        ConfigureAttributesStore.purchaseHistory = _purchaseHistory;
    }

    this.saveOptions = function() {
        console.log(this.options);
        for(var i in this.options) {
            var metric = i;
                _option = this.options[metric];

            this.purchaseHistory.forEach(function(item, key) { // delete previous obejcts for this kind
                if(item.metrics === metric) {
                    delete ConfigureAttributesStore.purchaseHistory[key];
                }
            });

            switch(metric) {
                case 'TotalSpendOvertime':
                case 'AvgSpendOvertime':
                    addSpendOvertimeSaveObject(metric, _option);
                    break;
                case 'SpendChange':
                    addDefaultSaveObject(metric, _option, true);
                    break;
                case 'ShareOfWallet':
                case 'Margin':
                default:
                    addDefaultSaveObject(metric, _option);
                    break;
            }
        }
        cleanPurchaseHistory();
        console.log(this.purchaseHistory);
    }
})
.service('ConfigureAttributesService', function($q, $http, $state, ResourceUtility) {
    this.getPurchaseHistory = function() {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '/pls/datacollection/metrics/PurchaseHistory/active',
            headers: { 'Content-Type': 'application/json' }
        }).success(function(result, status) {
            deferred.resolve(result);
        }).error(function(error, status) {
            console.log(error);
            deferred.resolve(error);
        });

        return deferred.promise;
        var deferred = $q.defer();
    };

    this.savePurchaseHistory = function(data) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: '/pls/datacollection/metrics/PurchaseHistory',
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).success(function(result, status) {
            deferred.resolve(result);
        }).error(function(error, status) {
            console.log(error);
            deferred.resolve(error);
        });

        return deferred.promise;
        var deferred = $q.defer();
    };
});
