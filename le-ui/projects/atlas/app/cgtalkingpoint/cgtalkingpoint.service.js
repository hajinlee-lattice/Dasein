angular.module('lp.cg.talkingpoint.talkingpointservice', [])
.service('CgTalkingPointStore', function($q, $rootScope, $timeout, CgTalkingPointService) {
    var CgTalkingPointStore = this;

    this.init = function() {
        this.danteUrl = null;
        this.accounts = null;
        this.danteAccounts = null;
        this.attributes = null;
        this.talkingPoints = [];
        this.talkingPointsPreviewResources = null;
        this.editedTalkingPoint = {};

        this.savedTalkingPoints = null;
        this.deleteClicked = false;
        this.calling = [];
    };
    this.init();

    this.saving = false;
    this.saveOnBlur = true;

    this.clear = function() {
        this.init();
    }

    this.getDeleteClicked = function() {
        return this.deleteClicked;
    }

    this.setEditedTalkingPoint = function(talkingPoint, propertyName) {
        if(talkingPoint && !propertyName) {
            this.editedTalkingPoint = talkingPoint;
        } else if(talkingPoint && propertyName && typeof talkingPoint === 'object') {
            this.editedTalkingPoint = talkingPoint;
            this.editedTalkingPoint[propertyName] = talkingPoint[propertyName];
        } else if(talkingPoint && propertyName && typeof talkingPoint === 'string') {
            this.editedTalkingPoint[propertyName] = talkingPoint;
        }
    };

    this.getEditedTalkingPoint = function(property) {
        if(property) {
            return (this.editedTalkingPoint[property] ? this.editedTalkingPoint[property] : null);
        }
        return this.editedTalkingPoint;
    };

    this.getTalkingPoint = function(name) {
        if(this.talkingPoints) {
            var talkingPoint = this.talkingPoints.find(function(item) {
                return (name === item.name);
            })
            return talkingPoint;
        }
    }

    this.isTalkingPointDirty = function(talkingPoint) {
        if(!talkingPoint) {
            return false;
        }
        var talkingPoint = angular.copy(talkingPoint),
            dirty = false,
            check = [
                'content',
                'title',
                'offset'
            ];

        if(!talkingPoint.pid) { // this means it's a new talking point
            return true; // "dirty"

        }

        // get the current talking point from the reference of saved talking points (i.e. not talking points from the mutable object)
        for(var i in this.savedTalkingPoints) {
            var currentTalkingPoint = this.savedTalkingPoints[i];
            if(currentTalkingPoint.name === talkingPoint.name) {
                var foundCurrentTalkingPoint = currentTalkingPoint;
                break;
            }
        }

        // just check from a whitelist to see if the properties are equal
        for(var i in check) {
            var property = check[i];
            if(foundCurrentTalkingPoint[property] !== talkingPoint[property]) {
                dirty = true;
                break;
            }
        };
        return dirty;
    }

    this.setTalkingPoints = function(talkingPoints) {
        this.talkingPoints = talkingPoints;
    };

    var expandValues = function(talkingPoints) {
        for(var i in talkingPoints) {
            talkingPoints[i].value = JSON.parse(talkingPoints[i].value);
        }
        return talkingPoints;
    }

    var stringifyValues = function(talkingPoints) {
        for(var i in talkingPoints) {
            talkingPoints[i].value = JSON.stringify(talkingPoints[i].value);
        }
        return talkingPoints;
    }

    this.getTalkingPoints = function(play_name, no_cache) {
        if(!play_name) {
            return this.talkingPoints;
        }
        var deferred = $q.defer();
        if(this.talkingPoints && this.talkingPoints.length && !no_cache) {
            deferred.resolve(this.talkingPoints);
        } else {
            CgTalkingPointService.getTalkingPoints(play_name).then(function(data){
                CgTalkingPointStore.setTalkingPoints(data);
                CgTalkingPointStore.savedTalkingPoints = angular.copy(data);
                deferred.resolve(data);
            });
        }
        return deferred.promise;
    };

    this.getTalkingPointsPreviewResources = function() {
        var callname = 'getTalkingPointsPreviewResources';
        if(!this.calling[callname]) {
            this.calling[callname] = true; // prevent double calling

            var deferred = $q.defer();
            // don't cache this because we always want a fresh oauth token every time
            CgTalkingPointService.getTalkingPointsPreviewResources().then(function(data){
                CgTalkingPointStore.talkingPointsPreviewResources = data;
                deferred.resolve(data);

                CgTalkingPointStore.calling[callname] = false;
            });
            return deferred.promise;
        }
    }

    this.setSavingFlag = function(bool) {
        this.saving = bool;
    }

    this.getSavingFlag = function() {
        return this.saving;
    }

    this.saveTalkingPoints = function(opts) {
        var deferred = $q.defer();
        var callname = 'saveTalkingPoints';
        if(!this.calling[callname]) {
            this.calling[callname] = true; // prevent double calling

            $rootScope.$broadcast('talkingPoints:saving');
            CgTalkingPointStore.setSavingFlag(true);

            CgTalkingPointService.saveTalkingPoints(opts).then(function(data){
                $rootScope.$broadcast('talkingPoints:saved');

                CgTalkingPointStore.setTalkingPoints(data);
                CgTalkingPointStore.savedTalkingPoints = angular.copy(data);
                CgTalkingPointStore.setSavingFlag(false);

                deferred.resolve(data);
                $timeout(function() {
                    CgTalkingPointStore.calling[callname] = false;
                }, 500);
            });
        }
        return deferred.promise;
    }

    this.deleteTalkingPoint = function(name) {
        $rootScope.$broadcast('talkingPoints:saving');
        var deferred = $q.defer();
        CgTalkingPointService.deleteTalkingPoint(name).then(function(data){
            $rootScope.$broadcast('talkingPoints:saved');
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.getDanteUrl = function(opts) {
        var deferred = $q.defer(),
            no_cache = opts.no_cache || false;
        if (this.danteUrl !== null && !no_cache) {
            deferred.resolve(this.danteUrl);
        } else {
        this.getTalkingPointsPreviewResources().then(function(data){
            CgTalkingPointService.getDanteUrl(data).then(function(response) {
                CgTalkingPointStore.danteUrl = response.data;
                deferred.resolve(response.data);
            });
        });
        }

        return deferred.promise;
    };

    var makeDanteAccountsObj = function(obj) {
        var accounts = [];
        obj.forEach(function(value, key){
            var nameToAdd = value.CompanyName;
            if(nameToAdd == null){
                nameToAdd = value.Website;
            }
            if(nameToAdd == null){
                nameToAdd = value.AccountId;
            }
            var tmpObj = {
                name: nameToAdd,
                id: value.AccountId
            };

            accounts.push(tmpObj);
        });
        return accounts;
    }

    this.getDanteAccounts = function(opts) {
        var deferred = $q.defer(),
            no_cache = opts.no_cache || false;
        if (this.danteAccounts !== null && !no_cache) {
            deferred.resolve(this.danteAccounts);
        } else {
            var self = this;
            CgTalkingPointService.getDanteAccounts(20, opts).then(function(response) {
                self.danteAccounts = makeDanteAccountsObj(response.data);
                deferred.resolve(self.danteAccounts);
            });
        }

        return deferred.promise;
    };

    this.getAccounts = function() {
        var query = {
            "free_form_text_search": "",
            "restrict_with_sfdcid": false,
            "restrict_without_sfdcid": false,
            "page_filter": {
                "row_offset": 0,
                "num_rows": 10
            }
        };
        
        return QueryStore.GetDataByQuery('accounts', query, PlaybookWizardStore.currentPlay.segment);
    };

    this.makeAttributesArray = function(attributes) {
        array = [];
        for(var i in attributes) {
            var key = i,
                value = attributes[i],
                attribute = {
                    name: key,
                    value: value
                };

            array.push(attribute);
        }
        return array;
    }

    this.getAttributes = function(entities) {
        var stub = false;
        var deferred = $q.defer();

        if (this.attributes !== null) {
            deferred.resolve(this.attributes);
        } else {
            if(stub) {
                CgTalkingPointService.getStubAttributes().then(function(response) {
                    CgTalkingPointStore.attributes = response;
                    deferred.resolve(CgTalkingPointStore.attributes);
                });
            } else {
                
                CgTalkingPointService.getAttributes(entities).then(function(response) {
                    // console.log('Entities',JSON.stringify(response.notionAttributes));
                    CgTalkingPointStore.attributes = response.notionAttributes;
                    deferred.resolve(CgTalkingPointStore.attributes);
                });
            }
 
        }
        return deferred.promise;
    };

    this.generateLeadPreviewObject = function(opts) {
        var deferred = $q.defer();
        CgTalkingPointService.getPreviewObject(opts).then(function(data){
            deferred.resolve(data);
        });
        return deferred.promise;
    };

    this.generateTalkingPoint = function(opts) {
        // {
        //     "name": "string" // random id, doesn't matter what it is
        //     "content": "string",
        //     "created": "2017-07-14T06:57:07.910Z",
        //     "name": "string",
        //     "offset": 0,
        //     "pid": 0,
        //     "playname": 0, // play name (id)
        //     "title": "string",
        //     "updated": "2017-07-14T06:57:07.911Z" // ISO date
        // }
        var opts = opts || {};
        opts.timestamp = opts.timestamp;

        var talkingPoint = {},
            ISOdate = (opts.timestamp ? new Date(opts.timestamp).toISOString() : '');

        talkingPoint.created = opts.creationDate || opts.timestamp;
        //talkingPoint.customerID = opts.customerID; //tenant id and will be removed eventially
        talkingPoint.name = opts.externalID || 'fakeId' + Math.round((Math.random()*10)*10000); // this will be removed someday I assume since this is supposed to be an internal id made by backend
        talkingPoint.playname = opts.playExternalID; // play_name (which is the play id)
        talkingPoint.pid = opts.pid;
        talkingPoint.offset = opts.offset;
        talkingPoint.title = opts.title;
        talkingPoint.content = opts.content;

        return talkingPoint;
    }

    this.publishTalkingPoints = function(play_name) {
        var deferred = $q.defer();
        CgTalkingPointService.publishTalkingPoints(play_name).then(function(data){
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.revertTalkingPoints = function(play_name) {
        var deferred = $q.defer();
        CgTalkingPointService.revertTalkingPoints(play_name).then(function(data){
            deferred.resolve(data);
        });
        return deferred.promise;
    }
})
.service('CgTalkingPointService', function($q, $http, $state) {
    this.host = '/pls'; //default

    this.getTalkingPoints = function(play_name){
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/dante/talkingpoints/play/'  + play_name
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.getTalkingPointsPreviewResources = function(){
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/dante/talkingpoints/previewresources'
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.saveTalkingPoints = function(opts) {
        var deferred = $q.defer();
        // for(var i in opts) {
        //     opts[i].created = new Date(opts[i].created).toISOString();
        // }
        $http({
            method: 'POST',
            url: this.host + '/dante/talkingpoints/',
            data: opts
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.deleteTalkingPoint = function(name) {
        var deferred = $q.defer();
        $http({
            method: 'DELETE',
            url: this.host + '/dante/talkingpoints/' + name,
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    };

    this.getDanteUrl = function(previewResources) {
        var deferred = $q.defer();
        var sessionid = previewResources.oAuthToken,
            preview_url = previewResources.danteUrl,
            server_url = previewResources.serverUrl,
            custom_settings_json = {
                hideNavigation: true,
                HideTabs: true,
                HideHeader: true
            },
            custom_settings = JSON.stringify(custom_settings_json).replace(/\"/g,'%22');

        deferred.resolve({data:preview_url + '?sessionid=' + sessionid + '&serverurl=' + server_url + '&CustomSettings=' + custom_settings + '&LpiPreview=true'});

        return deferred.promise;
    };

    this.getPreviewObject = function(opts) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/dante/talkingpoints/preview',
            params: {
                playName: opts.playName
            }
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    };

    this.getDanteAccounts = function(count, opts) {
        var deferred = $q.defer(),
            count = count || 20;
        var data = {
            'page_filter': { 
                'num_rows': count, 
                'row_offset': 0 
            }, 
            'restrict_with_sfdcid': false
        };
        
        var keys = Object.keys(opts);
        keys.forEach(function(key){
            data[key] = opts[key];
        });
        $http({
            // method: 'GET',
            // url: this.host + '/dante/accounts/' + count
            method: 'POST',
            url: this.host + '/accounts/data',
            data: data
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    };

    this.getStubAttributes = function() {
        var deferred = $q.defer();
        var data = {"data":{"Company":[{"value":"Account.Address1","name":"Address1"},{"value":"Account.Address2","name":"Address2"},{"value":"Account.City","name":"City"},{"value":"Account.Country","name":"Country"},{"value":"Account.DisplayName","name":"Company Name"},{"value":"Account.EstimatedRevenue","name":"Estimated Revenue"},{"value":"Account.LastModified","name":"Last Modification Date"},{"value":"Account.NAICSCode","name":"NAICS Code"},{"value":"Account.OwnerDisplayName","name":"Sales Rep"},{"value":"Account.SICCode","name":"SIC Code"},{"value":"Account.StateProvince","name":"StateProvince"},{"value":"Account.Territory","name":"Territory"},{"value":"Account.Vertical","name":"Industry"},{"value":"Account.Zip","name":"Zip"}],"Recommendation":[{"name":"Expected Value","value":"ExpectedValue"},{"name":"Likelihood","value":"LikelihoodBucketOffset"},{"name":"Campaign Name","value":"PlayDisplayName"},{"name":"Solution Name","value":"PlaySolutionName"},{"name":"Solution Type","value":"PlaySolutionType"},{"name":"Play Owner","value":"UserRoleDisplayName"},{"name":"Target Product","value":"PlayTargetProductName"},{"name":"Theme","value":"Theme"}]}};
        deferred.resolve(data.data);
        return deferred.promise;
    };

    this.getAttributes = function(entities) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/dante/attributes',
            data: entities
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    };

    this.getAccountAttributes = function() {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/dante/attributes/accountattributes'
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    };

    this.getRecommendationAttributes = function() {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/dante/attributes/recommendationattributes'
        }).then(function(response){
            deferred.resolve(response.data);
        });

        return deferred.promise;
    };

    this.publishTalkingPoints = function(play_name) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/dante/talkingpoints/publish',
            params: {
                playName: play_name
            }
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }

    this.revertTalkingPoints = function(play_name) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/dante/talkingpoints/revert',
            params: {
                playName: play_name
            }
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    }
});
