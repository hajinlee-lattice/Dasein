angular.module('lp.cg.talkingpoint.talkingpointservice', [])
.service('CgTalkingPointStore', function($q, CgTalkingPointService) {
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
    };
    this.init();

    this.saving = false;
    this.saveOnBlur = true;

    this.clear = function() {
        this.init();
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

    this.getEditedTalkingPoint = function() {
        return this.editedTalkingPoint;
    };

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
            if(!talkingPoint.title) {
                return false; // hack to not let it pass this test if it's new and doesn't have a title
            }
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
            property = check[i];
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

    this.getTalkingPointsPreviewResources = function(){
        var deferred = $q.defer();
        if(this.talkingPointsPreviewResources) {
            deferred.resolve(this.talkingPointsPreviewResources);
        } else {
            CgTalkingPointService.getTalkingPointsPreviewResources().then(function(data){
                CgTalkingPointStore.talkingPointsPreviewResources = data;
                deferred.resolve(data);
            });
        }
        return deferred.promise;
    }

    this.saveTalkingPoints = function(opts) {
        var deferred = $q.defer();
        this.saving = true;
        CgTalkingPointService.saveTalkingPoints(opts).then(function(data){
            CgTalkingPointStore.setTalkingPoints(data);
            CgTalkingPointStore.savedTalkingPoints = angular.copy(data);
            CgTalkingPointStore.saving = false;
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.deleteTalkingPoint = function(name) {
        var deferred = $q.defer();
        CgTalkingPointService.deleteTalkingPoint(name).then(function(data){
            //this.talkingPoints = opts;
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.getDanteUrl = function() {
        var deferred = $q.defer();
        if (this.danteUrl !== null) {
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
            value.value = (typeof value.value === 'string' ? JSON.parse(value.value) : value.value);
            var tmpObj = {
                name: value.value.DisplayName,
                id: value.value.BaseExternalID
            };
            accounts.push(tmpObj);
        });
        return accounts;
    }

    this.getDanteAccounts = function() {
        var deferred = $q.defer();

        if (this.danteAccounts !== null) {
            deferred.resolve(this.danteAccounts);
        } else {
            var self = this;
            CgTalkingPointService.getDanteAccounts().then(function(response) {
                self.danteAccounts = makeDanteAccountsObj(response);
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

    makeAttributesArray = function(attributes) {
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

    this.getAttributes = function() {
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
                var entities = ['account','recommendation'];
                CgTalkingPointService.getAttributes(entities).then(function(response) {
                    CgTalkingPointStore.attributes = response.notionAttributes;
                    deferred.resolve(CgTalkingPointStore.attributes);
                });
            }
 
        }
        return deferred.promise;
    };

    this.generateLeadPreviewObject = function(opts) {
        //return {"context":"lpipreview","notion":"lead","notionObject":{"ExpectedValue":UNKNOWN_UNTIL_LAUNCHED,"ExternalProbability":UNKNOWN_UNTIL_LAUNCHED,"LastLaunched":UNKNOWN_UNTIL_LAUNCHED,"Lift":UNKNOWN_UNTIL_LAUNCHED,"LikelihoodBucketDisplayName":UNKNOWN_UNTIL_LAUNCHED,"LikelihoodBucketOffset":UNKNOWN_UNTIL_LAUNCHED,"ModelID":null,"Percentile":UNKNOWN_UNTIL_LAUNCHED,"PlayDescription":PLACEHOLDER,"PlayDisplayName":PLACEHOLDER,"PlayID":PLACEHOLDER,"PlaySolutionType":null,"PlayTargetProductName":"D200-L","PlayType":"crosssell","Probability":UNKNOWN_UNTIL_LAUNCHED,"Rank":2,"Theme":"Sell Secure Star into Impravata owners","TalkingPoints":null}};
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
        var sessionid = previewResources.oAuthToken, //'00D80000000KvZo!AR0AQBWNZUrIO9q.DjIjFdXYW0USIN0SBQWCVvx0hw6naKZrc374OdQVP24EvFxZiWbf00dNdHjlPGFEScO4BMstUYEJlvka',
            preview_url = previewResources.danteUrl, //'https://localhost:44300/index.aspx',
            server_url = 'https://testapi.lattice-engines.com/api',//previewResources.serverUrl, //https://leinstallation.na6.visual.force.com/services/Soap/u/9.0/00D80000000KvZo&CustomSettings={%22hideNavigation%22:true,%22HideTabs%22:true,%22HideHeader%22:true}&LpiPreview=true'
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

    this.getDanteAccounts = function(count, segment) {
        var deferred = $q.defer(),
            count = count || 20;
        $http({
            method: 'GET',
            url: this.host + '/dante/accounts/' + count
        }).then(function(response){
            deferred.resolve(response.data);
        });
        return deferred.promise;
    };

    this.getStubAttributes = function() {
        var deferred = $q.defer();
        var data = {"data":{"Company":[{"value":"Account.Address1","name":"Address1"},{"value":"Account.Address2","name":"Address2"},{"value":"Account.City","name":"City"},{"value":"Account.Country","name":"Country"},{"value":"Account.DisplayName","name":"Company Name"},{"value":"Account.EstimatedRevenue","name":"Estimated Revenue"},{"value":"Account.LastModified","name":"Last Modification Date"},{"value":"Account.NAICSCode","name":"NAICS Code"},{"value":"Account.OwnerDisplayName","name":"Sales Rep"},{"value":"Account.SICCode","name":"SIC Code"},{"value":"Account.StateProvince","name":"StateProvince"},{"value":"Account.Territory","name":"Territory"},{"value":"Account.Vertical","name":"Industry"},{"value":"Account.Zip","name":"Zip"}],"Recommendation":[{"name":"Expected Value","value":"ExpectedValue"},{"name":"Likelihood","value":"LikelihoodBucketOffset"},{"name":"Play Name","value":"PlayDisplayName"},{"name":"Solution Name","value":"PlaySolutionName"},{"name":"Solution Type","value":"PlaySolutionType"},{"name":"Play Owner","value":"UserRoleDisplayName"},{"name":"Target Product","value":"PlayTargetProductName"},{"name":"Theme","value":"Theme"}]}};
        deferred.resolve(data.data);
        return deferred.promise;
    };

    this.getAttributes = function(entities) {
        var deferred = $q.defer();
        $http({
            method: 'POST',
            url: this.host + '/dante/attributes/attributes',
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
});
