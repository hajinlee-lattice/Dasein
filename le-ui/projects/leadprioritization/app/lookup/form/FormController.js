angular
.module('lp.lookup.form', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('LookupFormController', function($state, LookupStore, Models, ResourceUtility) {
    var vm = this;

    angular.extend(vm, {
        request: LookupStore.get('request'),
        ResourceUtility: ResourceUtility,
        models: Models
    });

    console.log(Models, LookupStore);

    vm.cancel = function() {
        $state.go('home.enrichments');
    }

    vm.next = function() {
        var timestamp = new Date().getTime();

        LookupStore.add('timestamp', timestamp);
        LookupStore.add('request', vm.request);
        
        $state.go('home.lookup.tabs');
    }
})
.service('LookupStore', function() {
    this.timestamp = 0;
    this.response = {};
    this.request = {
        modelId: '',
        record: {
            WebsiteAddress: 'www.lattice-engines.com',
            DUNSNumber: '',
            Id: '',
            EmailAddress: '',
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
            data: LookupStore.get('request'),
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            console.log('success', status, data);

            deferred.resolve(data);
        })
        .error(function(data, status, headers, config) {
            console.log('error', status, data);

            deferred.resolve(data);
        });

        return deferred.promise;
    }
});