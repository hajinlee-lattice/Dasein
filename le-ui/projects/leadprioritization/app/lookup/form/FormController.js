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
.service('LookupStore', function($sce) {
    this.timestamp = 0;
    this.params = { 
        shouldSkipLoadingEnrichmentMetadata: true,
        enforceFuzzyMatch: true 
    };
    this.response = {};
    this.request = {
        modelId: '',
        performEnrichment: true,
        record: {
            Domain: '',
            DUNS: '',
            Id: '',
            Email1: '',
            CompanyName: '',
            City: '',
            State: '',
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

    this.syntaxHighlight = function(json) {
        json = json ? json : '';
        if (typeof json != 'string') {
             json = JSON.stringify(json, undefined, 4);
        }
        json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        return $sce.trustAsHtml(json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
            var cls = 'number';
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    cls = 'key';
                } else {
                    cls = 'string';
                }
            } else if (/true|false/.test(match)) {
                cls = 'boolean';
            } else if (/null/.test(match)) {
                cls = 'null';
            }
            return '<span class="' + cls + '">' + match + '</span>';
        }));
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