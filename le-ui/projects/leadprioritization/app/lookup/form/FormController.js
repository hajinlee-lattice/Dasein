angular
.module('lp.lookup.form', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('LookupFormController', function($state, LookupStore, Models, ResourceUtility) {
    var vm = this;

    angular.extend(vm, {
        request: LookupStore.get('request'),
        params: LookupStore.get('params'),
        ResourceUtility: ResourceUtility,
        models: Models
    });
    vm.cancel = function() {
        $state.go('home.enrichments');
    }

    vm.next = function() {
        var timestamp = new Date().getTime();

        LookupStore.add('timestamp', timestamp);
        LookupStore.add('request', vm.request);
        LookupStore.add('params', vm.params);
        
        $state.go('home.lookup.tabs');
    }
})
.service('LookupStore', function() {
    this.timestamp = 0;
    this.params = { 
        enforceFuzzyMatch: true 
    };
    this.response = {};
    this.request = {
        modelId: '',
        performEnrichment: true,
        record: {
            Domain: 'www.lattice-engines.com',
            DUNS: '',
            Id: '',
            Email1: '',
            CompanyName: 'Lattice Engines',
            City: '',
            State: 'CA',
            Zip: '',
            County: '',
            PhoneNumber: '',
            City: ''
        }
    };

    this.add = function(type, request) {
        this[type] = request;
    }

    this.get = function(type) {
        return this[type];
    }
})
.service('LookupService', function($q, $http, LookupStore) {
    this.submit = function() {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/scores/apiconsole/record/debug',
            params: LookupStore.get('params'),
            data: LookupStore.get('request'),
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            deferred.resolve(data);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(data);
        });

        return deferred.promise;
    }
});