angular.module('lp.cg.talkingpoint.talkingpointservice', [])
.service('CgTalkingPointStore', function($q, CgTalkingPointService) {
    this.danteUrl = null;
    this.accounts = null;
    this.attributes = null;
    this.talkingPoints = [];

    this.setTalkingPoints = function(talkingPoints) {
        this.talkingPoints = talkingPoints;
    };

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
            deferred.resolve([]);
        }
        return deferred.promise;
    };

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
})
.service('CgTalkingPointService', function($q, $http, $state) {
    this.host = '/pls'; //default

    this.getTalkingPoints = function(play_name){
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: this.host + '/dantetalkingpoints/play/' + play_name
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
