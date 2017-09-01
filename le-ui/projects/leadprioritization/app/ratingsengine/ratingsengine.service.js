angular.module('lp.ratingsengine')
.service('RatingsEngineStore', function($q, $state, RatingsEngineService, BrowserStorageUtility){
    var RatingsEngineStore = this;

    this.init = function() {
        this.settings = {};
        this.validation = {
            segment: true,
            attributes: true,
            rules: true
        }
        this.currentRating = {};
        this.rating = null;
        this.ratings = null;
    }

    this.init();
    
    this.clear = function() {
        this.init();
    }

    this.setSettings = function(obj) {
        var obj = obj || {};
        for(var i in obj) {
            var key = i,
                value = obj[i];
            this.settings[key] = value;
        }
    }

    this.nextSaveGeneric = function(nextState) {
        var changed = false,
            opts = RatingsEngineStore.settings;

        RatingsEngineStore.saveRating().then(function(rating) {
            $state.go(nextState, {rating_id: rating.id});
        });
    }

    this.setRating = function(rating) {
        this.currentRating = rating;
    }

    this.getRating = function(id) {
        var deferred = $q.defer();
        if(this.rating) {
            deferred.resolve(this.rating)
        } else {
            RatingsEngineService.getRating(id).then(function(data) {
                RatingsEngineStore.setRating(data);
                deferred.resolve(data);
            });
        }
        return deferred.promise;
    }

    this.getCurrentRating = function() {
        return this.currentRating;
    }

    this.saveRating = function(opts) {
        var deferred = $q.defer(),
            opts = opts || {},
            ClientSession = BrowserStorageUtility.getClientSession();
        opts.createdBy = opts.createdBy || ClientSession.EmailAddress;
        opts.type = opts.type || 'RULE_BASED';
        RatingsEngineService.saveRating(opts).then(function(data){
            deferred.resolve(data);
            RatingsEngineStore.setRating(data);
        });
        return deferred.promise;
    }

    this.getRatings = function() {
        var deferred = $q.defer();
        RatingsEngineService.getRatingsStub().then(function(data) {
            RatingsEngineStore.ratings = data;
            deferred.resolve(data);
        });
        return deferred.promise;
    }

})
.service('RatingsEngineService', function($q, $http, $state) {
    this.host = '/pls'; //default

    this.getRatings = function() {
        var deferred = $q.defer(),
            result,
            url = this.host + '/ratingengines';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.deleteRating = function(ratingName) {

        var deferred = $q.defer(),
            result,
            url = '/pls/ratingengines/' + ratingName;

        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );
        return deferred.promise;
    }

    this.saveRating = function(opts) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/ratingengines',
            data: opts
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.getRating = function(id) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/ratingsengines/' + id
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.getRatingsStub = function() {
        var deferred = $q.defer(),
            stub = [{
            "created": "2017-08-28T17:11:47.490Z",
            "createdBy": "string",
            "displayName": "Jon Test Rating Engine",
            "id": "1234",
            "note": "string",
            "pid": 0,
            "ratingModels": [
            {
                "created": "2017-08-28T17:11:47.490Z",
                "id": "string",
                "iteration": 0,
                "ratingEngine": {
                    "created": "2017-08-28T17:11:47.490Z",
                    "createdBy": "string",
                    "displayName": "string",
                    "id": "string",
                    "note": "string",
                    "pid": 0,
                    "ratingModels": [
                    {}
                    ],
                    "segment": {
                        "created": "2017-08-28T17:11:47.490Z",
                        "description": "string",
                        "display_name": "string",
                        "frontend_restriction": {
                            "restriction": {
                                "childMap": {},
                                "children": [
                                {
                                    "childMap": {},
                                    "children": [
                                    {}
                                    ]
                                }
                                ]
                            }
                        },
                        "is_master_segment": true,
                        "masterSegment": true,
                        "name": "string",
                        "restriction": {
                            "childMap": {},
                            "children": [
                            {
                                "childMap": {},
                                "children": [
                                {}
                                ]
                            }
                            ]
                        },
                        "updated": "2017-08-28T17:11:47.499Z"
                    },
                    "status": "ACTIVE",
                    "tenant": {
                        "DisplayName": "string",
                        "Identifier": "string",
                        "RegisteredTime": 0,
                        "UIVersion": "string"
                    },
                    "type": "RULE_BASED",
                    "updated": "2017-08-28T17:11:47.499Z"
                },
                "updated": "2017-08-28T17:11:47.499Z"
            }
            ],
            "segment": {
                "created": "2017-08-28T17:11:47.499Z",
                "description": "string",
                "display_name": "string",
                "frontend_restriction": {
                    "restriction": {
                        "childMap": {},
                        "children": [
                        {
                            "childMap": {},
                            "children": [
                            {}
                            ]
                        }
                        ]
                    }
                },
                "is_master_segment": true,
                "masterSegment": true,
                "name": "string",
                "restriction": {
                    "childMap": {},
                    "children": [
                    {
                        "childMap": {},
                        "children": [
                        {}
                        ]
                    }
                    ]
                },
                "updated": "2017-08-28T17:11:47.499Z"
            },
            "status": "ACTIVE",
            "tenant": {
                "DisplayName": "string",
                "Identifier": "string",
                "RegisteredTime": 0,
                "UIVersion": "string"
            },
            "type": "RULE_BASED",
            "updated": "2017-08-28T17:11:47.499Z"
        }];
        deferred.resolve(stub);
        return deferred.promise;
    }

});
