angular.module('lp.cg.talkingpoint.talkingpointservice', [])
.service('CgTalkingPointStore', function($q, CgTalkingPointService) {
    this.danteUrl = null;
    this.accounts = null;
    this.attributes = null;
    this.talkingPoints = [];
    this.talkingPointsPreviewResources = null;

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

    this.getTalkingPoints = function(play_name) {
        if(!play_name) {
            return this.talkingPoints;
        }
        var deferred = $q.defer();
        if(this.talkingPoints.length) {
            deferred.resolve(this.talkingPoints);
        } else {
            CgTalkingPointService.getTalkingPoints(play_name).then(function(data){
                this.talkingPoints = data;
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
                this.talkingPointsPreviewResources = data;
                deferred.resolve(data);
            });
        }
        return deferred.promise;
    }

    this.saveTalkingPoints = function(opts) {
        var deferred = $q.defer();
        CgTalkingPointService.saveTalkingPoints(opts).then(function(data){
            this.talkingPoints = opts;
            deferred.resolve(data);
        });
        return deferred.promise;
    }

    this.deleteTalkingPoint = function(name) {
        console.log('delete', name);
        var deferred = $q.defer();
        CgTalkingPointService.deleteTalkingPoint(name).then(function(data){
            console.log(data);
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
            var self = this;
            CgTalkingPointService.getDanteUrl().then(function(response) {
                self.danteUrl = response.data;
                deferred.resolve(self.danteUrl);
            });
        }

        return deferred.promise;
    };

    this.getAccounts = function() {
        var deferred = $q.defer();

        if (this.accounts !== null) {
            deferred.resolve(this.accounts);
        } else {
            var self = this;
            CgTalkingPointService.getAccounts().then(function(response) {
                self.accounts = response.data;
                deferred.resolve(self.accounts);
            });
        }

        return deferred.promise;
    };

    this.getAttributes = function() {
        var deferred = $q.defer();

        if (this.attributes !== null) {
            deferred.resolve(this.attributes);
        } else {
            var self = this;
            CgTalkingPointService.getAttributes().then(function(response) {
                self.attributes = response.data;
                deferred.resolve(self.attributes);
            });
        }
        return deferred.promise;
    };

    this.generateLeadPreviewObject = function(play_name) {
        var PLACEHOLDER =UNKNOWN_UNTIL_LAUNCHED = null;
        return {"context":"lpipreview","notion":"lead","notionObject":{"ExpectedValue":UNKNOWN_UNTIL_LAUNCHED,"ExternalProbability":UNKNOWN_UNTIL_LAUNCHED,"LastLaunched":UNKNOWN_UNTIL_LAUNCHED,"Lift":UNKNOWN_UNTIL_LAUNCHED,"LikelihoodBucketDisplayName":UNKNOWN_UNTIL_LAUNCHED,"LikelihoodBucketOffset":UNKNOWN_UNTIL_LAUNCHED,"ModelID":null,"Percentile":UNKNOWN_UNTIL_LAUNCHED,"PlayDescription":PLACEHOLDER,"PlayDisplayName":PLACEHOLDER,"PlayID":PLACEHOLDER,"PlaySolutionType":null,"PlayTargetProductName":"D200-L","PlayType":"crosssell","Probability":UNKNOWN_UNTIL_LAUNCHED,"Rank":2,"Theme":"Sell Secure Star into Impravata owners","TalkingPoints":null}};
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

        talkingPoint.created = opts.creationDate || ISOdate;
        //talkingPoint.customerID = opts.customerID; //tenant id and will be removed eventially
        talkingPoint.name = opts.externalID || '123fakeStreet' + Math.random(); // this will be removed someday I assume since this is supposed to be an internal id made by backend
        talkingPoint.playname = opts.playExternalID; // play_name (which is the play id)
        talkingPoints.pid = opts.pid;
        talkingPoint.offset = opts.Offset;
        talkingPoint.title = opts.Title;
        talkingPoint.content = opts.Content;

        return talkingPoint;
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
            deferred.resolve(response.data.Result);
        });
        return deferred.promise;
    }

    this.getTalkingPointsPreviewResources = function(){
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/dante/talkingpoints/previewresources'
        }).then(function(response){
            deferred.resolve(response.data.Result);
        });
        return deferred.promise;
    }

    this.saveTalkingPoints = function(opts) {
        var deferred = $q.defer();
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
    }

    this.getDanteUrl = function() {
        var deferred = $q.defer();

        var sessionid = '00D80000000KvZo!AR0AQBWNZUrIO9q.DjIjFdXYW0USIN0SBQWCVvx0hw6naKZrc374OdQVP24EvFxZiWbf00dNdHjlPGFEScO4BMstUYEJlvka';
        deferred.resolve({data:'https://localhost:44300/index.aspx?sessionid='+sessionid+'&serverurl=https://leinstallation.na6.visual.force.com/services/Soap/u/9.0/00D80000000KvZo&CustomSettings={%22hideNavigation%22:true,%22HideTabs%22:true,%22HideHeader%22:true}&LpiPreview=true'});

        return deferred.promise;
    };

    this.getAccounts = function() {
        var deferred = $q.defer();

        var data = {
            data:[{
                name: 'Campus Management',
                id: '0018000000NW1EEAA1'
            }, {
                name: 'RTG Medical',
                id: '0018000000NW1EbAAL'
            }, {
                name: 'Omega Administrators',
                id: '0018000000NW1EUAA1'
            }, {
                name: 'AIT Laboratories',
                id: '0018000000NW1EOAA1'
            }]
        };
        deferred.resolve(data);

        return deferred.promise;
    };

    this.getAttributes = function() {
        var deferred = $q.defer();

        var data = {"data":{"Company":[{"value":"Account.Address1","name":"Address1"},{"value":"Account.Address2","name":"Address2"},{"value":"Account.City","name":"City"},{"value":"Account.Country","name":"Country"},{"value":"Account.DisplayName","name":"Company Name"},{"value":"Account.EstimatedRevenue","name":"Estimated Revenue"},{"value":"Account.LastModified","name":"Last Modification Date"},{"value":"Account.NAICSCode","name":"NAICS Code"},{"value":"Account.OwnerDisplayName","name":"Sales Rep"},{"value":"Account.SICCode","name":"SIC Code"},{"value":"Account.StateProvince","name":"StateProvince"},{"value":"Account.Territory","name":"Territory"},{"value":"Account.Vertical","name":"Industry"},{"value":"Account.Zip","name":"Zip"}],"Recommendation":[{"name":"Expected Value","value":"ExpectedValue"},{"name":"Likelihood","value":"LikelihoodBucketOffset"},{"name":"Play Name","value":"PlayDisplayName"},{"name":"Solution Name","value":"PlaySolutionName"},{"name":"Solution Type","value":"PlaySolutionType"},{"name":"Play Owner","value":"UserRoleDisplayName"},{"name":"Target Product","value":"PlayTargetProductName"},{"name":"Theme","value":"Theme"}]}};
        deferred.resolve(data);

        return deferred.promise;
    };
});
