angular.module('lp.delete')
.service('DeleteDataStore', function(){
    var DeleteDataStore = this;

    this.init = function() {
        this.fileUploadParams = { // need separate file upload params in order to use cancel method in fileMethod when resetting after successfully submitting the delete job
            BYUPLOAD_ID: {
                infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteAccountTemplate',
                operationType: 'BYUPLOAD_ID'
            },
            BYUPLOAD_MINDATEANDACCOUNT: {
                infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                operationType: 'BYUPLOAD_MINDATEANDACCOUNT'
            },
            BYUPLOAD_ACPD_1: {
                infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                operationType: 'BYUPLOAD_ACPD'
            },
            BYUPLOAD_ACPD_2: {
                infoTemplate: "<div class='row divider'><div class='twelve columns'><h4>What is a Training File?</h4><p>A training set is a CSV file with records of your historical successes. It is used to build your ideal customer profile by leveraging the Lattice Predictive Insights platform. Ideal training set should have at least 7,000 accounts, 150 success events and a conversion rate of less than 10%.</p></div></div><div class='row'><div class='six columns'><h4>Account Model:</h4><p>Upload a CSV file with accounts</p><p>Required: Id (any unique value for each record), Website (domain of company website), Event (1 for success, 0 otherwise)</p><p>Optional fields: Additional internal attributes about the accounts you would like to use as predictive attributes.</p></div><div class='six columns'><h4>Lead Model:</h4><p>Upload a CSV file with leads</p><p>Required: Id (any unique value for each record), Email, Event (1 for success, 0 otherwise)</p><p>Optional: Lead engagement data can be used as predictive attributes. Below are supported attributes:<ul><li>Marketo (4 week counts): Email Bounces (Soft), Email Clicks, Email Opens, Email Unsubscribes, Form Fills, Web-Link Clicks, Webpage Visits, Interesting Moments</li><li>Eloqua (4 week counts): Email Open, Email Send, Email Click Though, Email Subscribe, Email Unsubscribe, Form Submit, Web Visit, Campaign Membership, External Activity</li></ul></p></div></div>",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                operationType: 'BYUPLOAD_ACPD'
            }
        };
    }

    this.init();

    this.clear = function() {
        this.init();
    }

    this.getFileUploadParams = function() {
        return this.fileUploadParams;
    }
})
.service('DeleteDataService', function($q, $http) {

        this.cleanup = function(url, params) {
            console.log('url,', url);
            console.log('params,', params);
            var deferred = $q.defer();
            $http({
                method: 'POST',
                url: '/pls/cdl/' + url,
                params: params,
                headers: { 'Content-Type': 'application/json' }
            }).success(function(result, status) {
                deferred.resolve(result);
            }).error(function(error, status) {
                console.log(error);
                deferred.resolve(error);
            });  

            return deferred.promise;  
        }

    });
