angular.module('mainApp.core.services.NotionService', [
    'common.utilities.browserstorage',
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('NotionService', function ($http, $q, BrowserStorageUtility, ServiceErrorUtility, MetadataUtility, ResourceUtility) {
    
    this.findOne = function (notionName, notionId) {
        var self = this;
        var deferred = $q.defer();
        if (notionName == null || notionId == null) {
            deferred.resolve(null);
            return deferred.promise;
        }
        
        // First attempt to pull the notion data from localstorage
        var localData = $.jStorage.get(notionName);
        if (localData != null && localData.listData != null) {
            for (var i = 0; i < localData.listData.length; i++) {
                var notionData = localData.listData[i];
                if (notionData.Id == notionId && notionData.Timestamp > new Date().getTime()) {
                    var result = {
                        success: true,
                        resultObj: notionData.Data,
                        resultErrors: null
                    };
                    deferred.resolve(result);
                    return deferred.promise;
                }
            }
        }
        
        console.log('GetItem', notionName, notionId);
        
        //var getNotionUrl = "./DanteService.svc/GetItem?notionName=" + notionName;
        //getNotionUrl += "&id=" + notionId;

        var getNotionUrl = "/ulysses/datacollection/accounts/"+notionId+"/TalkingPoint/danteformat";
        
         $http({
            method: "GET", 
            url: getNotionUrl
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
            }
            
            var result = {
                success: data.Success,
                resultObj: null,
                resultErrors: null
            };
            if (data.Success === true) {
                if (data.Result != null) {
                    var resultObj = JSON.parse(data.Result);
                    self.storeNotionData(notionName, notionId, resultObj);
                    result.resultObj = resultObj;
                } else {
                    self.storeNotionData(notionName, notionId, null);
                }
            } else {
                result.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
        });
        
        return deferred.promise;
    };

    this.findOneByKey = function (notionName, id, idKey) {
        var self = this;
        var deferred = $q.defer();
        if (notionName == null || id == null) {
            deferred.resolve(null);
            return deferred.promise;
        }
        
        // First attempt to pull the notion data from localstorage
        var localData = $.jStorage.get(notionName);
        if (localData != null && localData.listData != null) {
            for (var i = 0; i < localData.listData.length; i++) {
                var notionData = localData.listData[i];
                if (notionData.Id == id && notionData.Timestamp > new Date().getTime()) {
                    var result = {
                        success: true,
                        resultObj: notionData.Data,
                        resultErrors: null
                    };
                    deferred.resolve(result);
                    return deferred.promise;
                }
            }
        }
        
        console.log('GetItemsByKey ONE', notionName, idKey, id);

        // old dante (c# api)
        var getNotionUrl = "./DanteService.svc/GetItemsByKey?notionName=" + notionName;
            getNotionUrl += "&keyname=" + idKey;
            getNotionUrl += "&value=" + id;
            getNotionUrl += "&limit=1";
        
        switch (notionName) {
            case "DanteLead":
                getNotionUrl = "/ulysses/recommendations/"+id+"/danteformat";
                
                break;
            
            case "DantePurchaseHistory":
                getNotionUrl = "/ulysses/purchasehistory/"+idKey+"/"+id+"/danteformat";
                //getNotionUrl = "/ulysses/purchasehistory/account/"+id+"/danteformat"
                //getNotionUrl = "/ulysses/purchasehistory/spendanalyticssegment/"+id+"/danteformat"
                break;
        }


        $http({
            method: "GET", 
            url: getNotionUrl
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
            }
            
            var result = {
                success: data.Success,
                resultObj: null,
                resultErrors: null
            };
            if (data.Success === true) {
                if (data.Result != null) {
                    var resultObj = JSON.parse(data.Result);
                    self.storeNotionData(notionName, id, resultObj);
                    result.resultObj = resultObj;
                } else {
                    self.storeNotionData(notionName, id, null);
                }
            } else {
                result.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
        });
        
        return deferred.promise;
    };

    this.findItemsByKey = function (notionName, id, idKey, limit) {
        var deferred = $q.defer();
        if (notionName == null || id == null || idKey == null) {
            deferred.resolve(null);
            return deferred.promise;
        }
        
        // First attempt to pull the notion data from localstorage
        var localStorageKey = notionName + "-" + id;
        var localData = $.jStorage.get(localStorageKey);
        if (localData != null && localData.Timestamp > new Date().getTime()) {
            deferred.resolve(localData.listData);
        } else {
            console.log('GetItemsByKey', notionName, idKey, id, limit);

            var getNotionUrl = "./DanteService.svc/GetItemsByKey?notionName=" + notionName;
            getNotionUrl += "&keyname=" + idKey;
            getNotionUrl += "&value=" + id;
            getNotionUrl += "&limit=" + (limit || -1);

            switch (notionName) {
                case "DanteTalkingPoint":
                    getNotionUrl = "/ulysses/talkingpoints/playid/"+id+"/danteformat";
                    
                    break;
                
                case "DanteAccount":
                    if (idKey === "IsSegment") {
                        getNotionUrl = "/ulysses/datacollection/accounts/spendanalyticssegments/danteformat";
                    }

                    break;
            }
        
            $http({
                method: "GET", 
                url: getNotionUrl
            })
            .success(function(data, status, headers, config) {
                if (data == null) {
                    deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
                }
                
                var resultObj = {
                    listData: [],
                    Timestamp: new Date().getTime() + BrowserStorageUtility.CacheTimeout, // Set a Timestamp to determine how long the stored object is valid
                    resultErrors: null
                };
                if (data.Success === true && data.Result != null) {
                    for (var i = 0; i < data.Result.length; i++) {
                        resultObj.listData.push(JSON.parse(data.Result[i]));
                    }
                    $.jStorage.set(localStorageKey, resultObj);
                } else {
                    resultObj.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
                }
                deferred.resolve(resultObj.listData);
            })
            .error(function(data, status, headers, config) {
                deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
            });
        }
        
        return deferred.promise;
    };
    
    this.storeNotionData = function (notionName, notionId, notionData) {
        var storedNotionObject = {
            Id: notionId,
            Data: notionData,
            Timestamp: new Date().getTime() + BrowserStorageUtility.CacheTimeout // Set a Timestamp to determine how long the stored object is valid
        };

        var localData = $.jStorage.get(notionName);
        if (localData === null || localData.listData === null || localData.listData.length === 0) {
        //if (false) {
            var storedNotionList = {
                listData: []
            };
            storedNotionList.listData.push(storedNotionObject);
            console.log('new storeNotionData();', notionName, storedNotionList);
            $.jStorage.set(notionName, storedNotionList);
        } else {
            console.log('cached storeNotionData();', notionName, notionId, storedNotionObject);
            var foundNotion = false;
            var notion;
            for (var i = 0; i < localData.listData.length; i++) {
                notion = localData.listData[i];
                if (notion.Id == notionId) {
                    foundNotion = true;
                    break;
                }
            }
            
            if (foundNotion === true) {
                notion.Data = notionData;
                notion.Timestamp = storedNotionObject.Timestamp;
            } else {
                localData.listData.push(storedNotionObject);
            }
            $.jStorage.set(notionName, localData);
        }
    };

    this.findAll = function (notionName) {
        var deferred = $q.defer();
        if (notionName == null) {
            deferred.resolve(null);
            return deferred.promise;
        }
        
        // First attempt to pull the notion data from localstorage
        var localData = $.jStorage.get(notionName);

        if (localData != null && localData.Timestamp > new Date().getTime()) {
            deferred.resolve(localData.listData);
        } else {
            console.log('GetAllItems', notionName);
            
            //var getNotionUrl = "./DanteService.svc/GetAllItems?notionName=" + notionName;
            var getNotionUrl = "/ulysses/producthierarchy/danteformat";

            $http({
                method: "GET", 
                url: getNotionUrl
            })
            .success(function(data, status, headers, config) {
                if (data === null) {
                    deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
                }
                
                var resultObj = {
                    listData: [],
                    Timestamp: new Date().getTime() + BrowserStorageUtility.CacheTimeout, // Set a Timestamp to determine how long the stored object is valid
                    resultErrors: null
                };
                if (data.Success === true && data.Result !== null) {
                    for (var i = 0; i < data.Result.length; i++) {
                        resultObj.listData.push(JSON.parse(data.Result[i]));
                    }
                    $.jStorage.set(notionName, resultObj);
                } else {
                    resultObj.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
                }
                deferred.resolve(resultObj.listData);
            })
            .error(function(data, status, headers, config) {
                deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
            });
        }
        
        return deferred.promise;
    };
    
    /*
    * keyValuePairs: array of key value pairs [{"Key": column_name, "Value": value},...]
    */
    this.findItemsByKeys = function (notionName, keyValuePairs, limit) {
        var deferred = $q.defer();
        if (notionName == null || !Array.isArray(keyValuePairs) || keyValuePairs.length === 0) {
            deferred.resolve(null);
            return deferred.promise;
        }

        var keyValuePairsQuery = JSON.stringify(keyValuePairs);
        var localStorageKey = notionName + '-' + keyValuePairsQuery; 
        var localData = $.jStorage.get(localStorageKey);

        if (localData != null && localData.Timestamp > new Date().getTime()) {
            deferred.resolve(localData.listData);
            return deferred.promise;
        } else {
            console.log('GetItemsByKeys', notionName, keyValuePairs, limit)
            var getNotionUrl = "./DanteService.svc/GetItemsByKeys?notionName=" + notionName;
            getNotionUrl += "&keyValuePairs=" + keyValuePairsQuery;
            getNotionUrl += "&limit=" + (limit || -1);
            
            $http({
                method: "GET", 
                url: getNotionUrl
            })
            .success(function(data, status, headers, config) {
                if (data == null) {
                    deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
                }
                
                var resultObj = {
                    listData: [],
                    Timestamp: new Date().getTime() + BrowserStorageUtility.CacheTimeout, // Set a Timestamp to determine how long the stored object is valid
                    resultErrors: null
                };
                if (data.Success === true && data.Result != null) {
                    for (var i = 0; i < data.Result.length; i++) {
                        resultObj.listData.push(JSON.parse(data.Result[i]));
                    }
                    $.jStorage.set(localStorageKey, resultObj);
                } else {
                    resultObj.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
                }
                deferred.resolve(resultObj.listData);
            })
            .error(function(data, status, headers, config) {
                deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
            });
        }

        return deferred.promise;
    };


    this.setRootNotion = function (data, widgetConfig) {
        var metadata = BrowserStorageUtility.getMetadata();
        if (data == null || widgetConfig == null || metadata == null) {
            return {};
        }

        // Find the appropriate notion metadata
        var notionMetadata = MetadataUtility.GetNotionMetadata(widgetConfig.Notion, metadata);

        // Find Play Name property from the metadata
        var headerNameProperty = MetadataUtility.GetNotionProperty(widgetConfig.HeaderNameProperty, notionMetadata);
        if (headerNameProperty != null && headerNameProperty.PropertyTypeString == MetadataUtility.PropertyType.STRING) {
            data.HeaderName = data[headerNameProperty.Name] || "";
        } else {
            data.HeaderName = ResourceUtility.getString('DANTE_GENERIC_HEADER_LABEL');
        }
        BrowserStorageUtility.setCurrentRootObject(data);
    };
});