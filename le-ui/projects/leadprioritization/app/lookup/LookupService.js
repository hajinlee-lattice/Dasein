angular
.module('lp.lookup.form')
.service('LookupStore', function($sce, FeatureFlagService) {
    var LookupStore = this;

    this.count = 0;
    this.timestamp = 0;
    this.params = { 
        shouldSkipLoadingEnrichmentMetadata: true,
        enforceFuzzyMatch: true
    };
    this.response = {};
    this.request = {
        modelId: '',
        performEnrichment: true,
        record: { }
    };

    this.setParam = function(property, value) {
        this.params[property] = value;
    }

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