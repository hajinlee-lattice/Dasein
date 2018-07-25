angular.module('lp.delete')
.service('DeleteDataStore', function($sce){
    var DeleteDataStore = this;

    this.init = function() {
        this.fileUploadParams = { // need separate file upload params in order to use cancel method in fileMethod when resetting after successfully submitting the delete job
            BYUPLOAD_ID: {
                // accountInfoTemplate: "<div> <div class='twelve columns'> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Account Data. All the fields are required.</p> </div></div><div class='row'> <div style='margin-top: 0px;'> <table> <thead> <tr> <th>Account ID</th> </tr> </thead> <tbody> <tr> <td>1</td> </tr> <tr> <td>2</td> <tr> <td>3</td> </tbody> </table> </div></div>",
                // accountInfoTemplate: '<div> <div class="twelve columns"> <h5>Sample File Format</h5> <p style="margin-top:5px;">Upload a CSV file using the following data format to delete your Account Data. All the fields are required.</p> </div></div> <div class="row"> <div style="margin-top: 5px;"> <table> <thead> <tr> <th>Account ID</th> </tr> </thead> <tbody> <tr> <td>1</td> </tr> <tr> <td>2</td> <tr> <td>3</td> </tbody> </table> </div></div>',
                // accountInfoTemplate: '"<div class=\"row divider\"> <div class=\"twelve columns\"> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Account Data. All the fields are required.</p> </div></div><div class=\"row\"> <div style=\"margin-top: 0px;\"> <table> <thead> <tr> <th>Account ID</th> </tr> </thead> <tbody> <tr> <td>1</td> </tr> <tr> <td>2</td> <tr> <td>3</td> </tbody> </table> </div></div>"',
                // accountInfoTemplate: $sce.trustAsHtml('\"<div class=\"row divider\"> <div class=\"twelve columns\"> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Account Data. All the fields are required.</p> </div></div><div class=\"row\"> <div style=\"margin-top: 0px;\"> <table> <thead> <tr> <th>Account ID</th> </tr> </thead> <tbody> <tr> <td>1</td> </tr> <tr> <td>2</td> <tr> <td>3</td> </tbody> </table> </div></div>\"'),
                // accountInfoTemplate: '<p> Hello </p>',
                accountInfoTemplate: "<div class='template-section'> <div class='twelve columns'> <h4>Sample File Format</h4> <p class='template-description'>Upload a CSV file using the following data format to delete your Account Data. All the fields are required.</p></div></div><div class='template-section row'> <div> <table class='medium'> <thead> <tr> <th>Account ID</th> </tr></thead> <tbody> <tr> <td>1</td></tr><tr> <td>2</td><tr> <td>3</td></tbody> </table> </div></div>",
                // accountInfoTemplate: '<div><div class="columns twelve"><h5>Sample File Format</h5><p>Upload a CSV file using the following data format to delete your Account Data. All the fields are required.</div></div><div class=row><div><table><thead><tr><th>Account ID<tbody><tr><td>1<tr><td>2<tr><td>3</table></div></div>',
                contactInfoTemplate: "<div class='template-section'> <div class='twelve columns'> <h4>Sample File Format</h4> <p class='template-description'>Upload a CSV file using the following data format to delete your Contact Data. All the fields are required.</p> </div></div><div class='template-section row'> <div> <table class='medium'> <thead> <tr> <th>Contact ID</th> </tr> </thead> <tbody> <tr> <td>1</td> </tr> <tr> <td>2</td> <tr> <td>3</td> </tbody> </table> </div></div>",
                accountLabel: "Upload a single csv file with Account IDs",
                contactLabel: "Upload a single csv file with Contact IDs",
                tooltipConfiguration: {
                    whiteBackground: true,
                    tooltipSide: 'top',
                    tooltipSize: 'large'
                },
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteAccountTemplate',
                whiteBackground: true,
                operationType: 'BYUPLOAD_ID'
            },
            BYUPLOAD_MINDATEANDACCOUNT: {
                // infoTemplate: "<div class='row divider'> <div class='twelve columns'> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Transaction Data. All the fields are required.</p> </div></div><div class='row'> <div style='margin-top: 0px;'> <table> <thead> <tr> <th>Account ID</th> <th>TransactionTime</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>4/1/17 0:00</td> </tr> <tr> <td>2</td> <td>11/1/15 0:00</td> </tr> <tr> <td>3</td> <td>8/1/15 0:00</td> </tr> </tbody> </table> </div></div>",
                infoTemplate: "<div class='template-section'> <div class='twelve columns'> <h4>Sample File Format</h4> <p class='template-description'>Upload a CSV file using the following data format to delete your Transaction Data. All the fields are required.</p></div></div><div class='template-section row'> <div> <table class='medium'> <thead> <tr> <th>Account ID</th> <th>TransactionTime</th> </tr></thead> <tbody> <tr> <td>1</td><td>4/1/17 0:00</td></tr><tr> <td>2</td><td>11/1/15 0:00</td></tr><tr> <td>3</td><td>8/1/15 0:00</td></tr></tbody> </table> </div></div>",
                tooltipConfiguration: {
                    whiteBackground: true,
                    tooltipSide: 'top',
                    tooltipSize: 'large'
                },   
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                operationType: 'BYUPLOAD_MINDATEANDACCOUNT'
            },
            BYUPLOAD_ACPD_1: {
                infoTemplate: "<div class='template-section'> <div class='twelve columns'> <h4>Sample File Format</h4> <p class='template-description'>Upload a CSV file using the following data format to delete your Transaction Data. All the fields are required.</p> </div></div><div class='template-description row'> <div> <table class='medium'> <thead> <tr> <th>Account ID</th> <th>Product ID</th> <th>TransactionTime</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>1</td> <td>4/1/17 0:00</td> </tr> <tr> <td>1</td> <td>2</td> <td>11/1/15 0:00</td> </tr> <tr> <td>1</td> <td>3</td> <td>8/1/15 0:00</td> </tr> </tbody> </table> </div></div>",
                tooltipConfiguration: {
                    whiteBackground: true,
                    tooltipSide: 'top',
                    tooltipSize: 'large'
                },
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                operationType: 'BYUPLOAD_ACPD'
            },
            BYUPLOAD_ACPD_2: {
                infoTemplate: "<div class='template-section'> <div class='twelve columns'> <h4>Sample File Format</h4> <p class='template-description'>Upload a CSV file using the following data format to delete your Transaction Data. All the fields are required.</p> </div></div><div class='template-section row'> <div> <table class='medium'> <thead> <tr> <th>Account ID</th> <th>Product ID</th> <th>Contact ID</th> <th>TransactionTime</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>1</td> <td>1</td> <td>4/1/17 0:00</td> </tr> <tr> <td>2</td> <td>2</td> <td>1</td> <td>11/1/15 0:00</td> </tr> <tr> <td>2</td> <td>3</td> <td>1</td> <td>8/1/15 0:00</td> </tr> </tbody> </table> </div></div>",
                tooltipConfiguration: {
                    whiteBackground: true,
                    tooltipSide: 'top',
                    tooltipSize: 'large'
                },
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                whiteBackground: true,
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
