angular
.module('lp.campaigns')
.service('CampaignService', function($http, $q, $state) {
    this.GetCampaigns = function(id) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url = '/pls/campaigns/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.GetCampaignById = function(campaignId) {
        var deferred = $q.defer(),
            result,
            url = '/pls/campaigns/' + campaignId;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.CreateCampaign = function(campaign) {
        var deferred = $q.defer(),
            data = {
                name: campaign.campaignName,
                description: campaign.campaignDescription
            },
            url = '/pls/campaigns/' + data.name;

        $http({
            method: 'POST',
            url: url,
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result); 
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.DeleteCampaign = function(campaignId) {
        var deferred = $q.defer(),
            result = {},
            url = '/pls/campaigns/' + campaignId;

        $http({
            method: 'DELETE',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

});
