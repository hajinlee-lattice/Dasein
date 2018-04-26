angular.module('lp.delete')
.service('DeleteDataStore', function(){
    var DeleteDataStore = this;

    this.init = function() {
        this.fileUploadParams = { // need separate file upload params in order to use cancel method in fileMethod when resetting after successfully submitting the delete job
            BYUPLOAD_ID: {
                accountInfoTemplate: "<div class='row divider'> <div class='twelve columns'> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Trasaction Data. All the fields are required.</p> </div></div><div class='row'> <div style='margin-top: 0px;'> <table> <thead> <tr> <th>Account ID</th> <th>Account Name</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>Intel</td> </tr> <tr> <td>2</td> <td>Edge Communications</td> </tr> <tr> <td>3</td> <td>Burlington Textiles Corp</td> </tr> </tbody> </table> </div></div>",
                contactInfoTemplate: "<div class='row divider'> <div class='twelve columns'> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Trasaction Data. All the fields are required.</p> </div></div><div class='row'> <div style='margin-top: 0px;'> <table> <thead> <tr> <th>Contact ID</th> <th>Account ID</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>1</td> </tr> <tr> <td>2</td> <td>1</td> <tr> <td>3</td> <td>1</td> </tbody> </table> </div></div>",
                accountLabel: "Upload a single csv file with Account IDs",
                contactLabel: "Upload a single csv file with Contact IDs",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteAccountTemplate',
                operationType: 'BYUPLOAD_ID'
            },
            BYUPLOAD_MINDATEANDACCOUNT: {
                infoTemplate: "<div class='row divider'> <div class='twelve columns'> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Trasaction Data. All the fields are required.</p> </div></div><div class='row'> <div style='margin-top: 0px;'> <table> <thead> <tr> <th>Account ID</th> <th>Transaction Date</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>4/1/17 0:00</td> </tr> <tr> <td>2</td> <td>11/1/15 0:00</td> </tr> <tr> <td>3</td> <td>8/1/15 0:00</td> </tr> </tbody> </table> </div></div>",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                operationType: 'BYUPLOAD_MINDATEANDACCOUNT'
            },
            BYUPLOAD_ACPD_1: {
                infoTemplate: "<div class='row divider'> <div class='twelve columns'> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Trasaction Data. All the fields are required.</p> </div></div><div class='row'> <div style='margin-top: 0px;'> <table> <thead> <tr> <th>Account ID</th> <th>Product ID</th> <th>Transaction Date</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>1</td> <td>4/1/17 0:00</td> </tr> <tr> <td>1</td> <td>2</td> <td>11/1/15 0:00</td> </tr> <tr> <td>1</td> <td>3</td> <td>8/1/15 0:00</td> </tr> </tbody> </table> </div></div>",
                compressed: true,
                importError: false,
                importErrorMsg: '',
                url: '/pls/models/uploadfile/uploaddeletefiletemplate',
                schema: 'DeleteTransactionTemplate',
                operationType: 'BYUPLOAD_ACPD'
            },
            BYUPLOAD_ACPD_2: {
                infoTemplate: "<div class='row divider'> <div class='twelve columns'> <h4>Sample File Format</h4> <p>Upload a CSV file using the following data format to delete your Trasaction Data. All the fields are required.</p> </div></div><div class='row'> <div style='margin-top: 0px;'> <table> <thead> <tr> <th>Account ID</th> <th>Product ID</th> <th>Contact ID</th> <th>TransactionTime</th> </tr> </thead> <tbody> <tr> <td>1</td> <td>1</td> <td>1</td> <td>4/1/17 0:00</td> </tr> <tr> <td>2</td> <td>2</td> <td>1</td> <td>11/1/15 0:00</td> </tr> <tr> <td>2</td> <td>3</td> <td>1</td> <td>8/1/15 0:00</td> </tr> </tbody> </table> </div></div>",
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
