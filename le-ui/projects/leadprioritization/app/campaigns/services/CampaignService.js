angular.module('mainApp.campaigns.services.CampaignService', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('CampaignStore', function($q, $timeout, CampaignService) {
    var CampaignStore = this;

    this.campaigns = [];
    this.campaignsMap = {};
    this.stale = true;

    // checks if items matching args exists, performs XHR to fetch if they don't
    this.getCampaign = function(campaignName) {
        var deferred = $q.defer(),
            campaign = this.campaignsMap[campaignName];
        
        if (typeof campaign == 'object') {
            deferred.resolve(campaign);
        } else {
            CampaignService.GetCampaignById(campaignName).then(function(result) {
                if (result != null && result.success === true) {
                    CampaignStore.addModel(campaignName, result.resultObj);
                    deferred.resolve(result.resultObj);
                } else {
                    deferred.reject(result.resultObj);
                }
            });
        }

        return deferred.promise;
    };

    this.getCampaigns = function(use_cache) {
        var deferred = $q.defer();

        if (use_cache && CampaignStore.campaigns.length > 0) {
            deferred.resolve(CampaignStore.campaigns);
        } else if (this.stale) {
            CampaignService.GetAllCampaigns().then(function(response) {
                var campaigns = response.resultObj;

                if (!campaigns) {
                    ServiceErrorUtility.process(response);
                    return;
                }

                CampaignStore.campaigns.length = 0;

                campaigns.forEach(function(campaign, index) {
                    CampaignStore.campaigns.push(campaign);
                });

                CampaignStore.stale = false;

                $timeout(function() {
                    CampaignStore.stale = true;
                }, 500);

                deferred.resolve(campaigns);
            });
        } else {
            deferred.resolve(CampaignStore.campaigns);
        }

        return deferred.promise;
    };


})
.service('CampaignService', function ($http, $q, _, ResourceUtility) {

    this.GetAllCampaigns = function () {
            var deferred = $q.defer();
            var result;
            var request;
            request = {
                method: 'GET',
                url: '/pls/campaigns/',
                headers: {
                    "Content-Type": "application/json"
                }
            };
            $http(request)
            .success(function(data, status, headers, config) {
                if (data == null) {
                    result = {
                        success: false,
                        resultObj: null,
                        resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                    };
                    deferred.resolve(result);
                } else {
                    result = {
                        success: true,
                        resultObj: null,
                        resultErrors: null
                    };

                    data = _.sortBy(data, 'ConstructionTime').reverse();
                    // sync with front-end json structure
                    result.resultObj = _.map(data, function(rawObj) {
                            return {
                                name : rawObj.DisplayName == null || rawObj.DisplayName == "" ? rawObj.Name : rawObj.DisplayName,
                                campaign_type: rawObj.CampaignType
                            };}
                    );

                }
                deferred.resolve(result);
            })
            .error(function(data, status, headers, config) {
                SessionService.HandleResponseErrors(data, status);
                if (status == 403) {
                    result = {
                        success: true,
                        resultObj: null,
                        resultErrors: null
                    };
                } else {
                    result = {
                        success: false,
                        resultObj: null,
                        resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                    };
                }
                deferred.resolve(result);
            });

        return deferred.promise;
    };

    this.CreateAccountCampaign = function () {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'PUT',
            url: '/pls/campaigns/'+ campaignName,
            data: { name: campaignName },
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    Success: true,
                    ResultErrors: null
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_TILE_EDIT_SERVICE_ERROR')
            };
            if (data.errorCode == 'LEDP_18003') result.ResultErrors = ResourceUtility.getString('CHANGE_CAMPAIGN_NAME_ACCESS_DENIED');
            if (data.errorCode == 'LEDP_18014') result.ResultErrors = ResourceUtility.getString('CHANGE_CAMPAIGN_NAME_CONFLICT');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.ChangeCampaignDisplayName = function (campaignName) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'PUT',
            url: '/pls/campaigns/'+ campaignName,
            data: { name: campaignName },
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    Success: false,
                    ResultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    Success: true,
                    ResultErrors: null
                };
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                Success: false,
                ResultErrors: ResourceUtility.getString('MODEL_TILE_EDIT_SERVICE_ERROR')
            };
            if (data.errorCode == 'LEDP_18003') result.ResultErrors = ResourceUtility.getString('CHANGE_CAMPAIGN_NAME_ACCESS_DENIED');
            if (data.errorCode == 'LEDP_18014') result.ResultErrors = ResourceUtility.getString('CHANGE_CAMPAIGN_NAME_CONFLICT');
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.validateCampaignName = function(name) {
        var result = {
            valid: false,
            errMsg: null
        };
        if (name.replace(/ /g,'') === "") {
            result.errMsg = ResourceUtility.getString('CAMPAIGN_TILE_EDIT_TITLE_EMPTY_ERROR');
            return result;
        }
        if (name.length > 50) {
            result.errMsg = ResourceUtility.getString('CAMPAIGN_TILE_EDIT_TITLE_LONG_ERROR');
            return result;
        }
        result.valid = true;
        return result;
    };
});
