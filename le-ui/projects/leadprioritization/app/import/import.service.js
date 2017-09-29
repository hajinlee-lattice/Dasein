angular.module('lp.import')
.service('ImportWizardStore', function($q, ImportWizardService){
    var ImportWizardStore = this;

    this.csvFileName = null;
    this.fieldDocument = null;
    this.unmappedFields = null;
    this.accountIdState = {
        accountDedupeField: null,
        dedupeType: 'custom',
        selectedField: null,
        fields: ['Id']
    };

    this.validation = {
        
    }

    this.getAccountIdState = function() {
        return this.accountIdState;
    };

    this.setAccountIdState = function(nextState) {
        for (var key in this.accountIdState) {
            this.accountIdState[key] = nextState[key];
        }
    };

    this.getCsvFileName = function() {
        return this.csvFileName;
    };
    this.setCsvFileName = function(fileName) {
        this.csvFileName = fileName;
    };

    this.getFieldDocument = function() {
        return this.fieldDocument;
    };

    this.getFieldDocumentAttr = function(name) {
        if(name == 'fieldMappings') {
            return this.fieldDocument.fieldMappings;
        } else if(name == 'ignoredFields') {
            return this.fieldDocument.ignoredFields;
        }
    };
    this.setFieldDocument = function(data) {
        this.fieldDocument = data;
    };

    this.getUnmappedFields = function() {
        return this.unmappedFields;
    };
    this.setUnmappedFields = function(data) {
        this.unmappedFields = data;
    };

    this.getCustomFields = function(type) {
        var data = [],
            total = 7, //Math.floor(Math.random() * 10 + 1),
            types = ['Text', 'Number', 'Boolean', 'Date'];
        for(var i=0;i<total;i++) {
            var tmp = {
                CustomField: 'CustomField' + (i + 1),
                Type: types, //[Math.floor(Math.random()*types.length)],
                Ignore: false //Math.random() >= 0.5
            };
            data.push(tmp);
        }
        return data;
    }
})
.service('ImportWizardService', function($q, $http, $state, ResourceUtility) {

	this.GetSchemaToLatticeFields = function(csvFileName) {
	        var deferred = $q.defer();
	        var params = { 'entity':  'Account' };

	        $http({
	            method: 'GET',
	            url: '/pls/models/uploadfile/latticeschema/cdl',
	            params: params,
	            headers: { 'Content-Type': 'application/json' }
	        }).then(function(data) {
	            deferred.resolve(data.data.Result);
	        });

	        return deferred.promise;
	    };

	    this.GetFieldDocument = function(FileName) {
	        var deferred = $q.defer();
	        var entity = "account";
	        var params =  { 'entity': entity };

	        $http({
	            method: 'POST',
	            url: '/pls/models/uploadfile/' + FileName + '/fieldmappings/cdl',
	            params: params,
	            headers: { 'Content-Type': 'application/json' },
	        })
	        .success(function(data, status, headers, config) {
	            if (data == null || !data.Success) {
	                if (data && data.Errors.length > 0) {
	                    var errors = data.Errors.join('\n');
	                }
	                result = {
	                    Success: false,
	                    ResultErrors: errors || ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
	                    Result: null
	                };
	            } else {
	                result = {
	                    Success: true,
	                    ResultErrors: data.Errors,
	                    Result: data.Result
	                };
	            }

	            deferred.resolve(result);
	        })
	        .error(function(data, status, headers, config) {
	            var result = {
	                Success: false,
	                ResultErrors: data.errorMsg
	            };

	            deferred.resolve(result);
	        });

	        return deferred.promise;
	    };

	    this.SaveFieldDocuments = function(FileName, FieldDocument) {
	        var deferred = $q.defer();
	        var result;
	        var params = { 'displayName': FileName };

	        $http({
	            method: 'POST',
	            url: '/pls/models/uploadfile/fieldmappings',
	            headers: { 'Content-Type': 'application/json' },
	            params: params,
	            data: {
	                'fieldMappings': FieldDocument.fieldMappings,
	                'ignoredFields': FieldDocument.ignoredFields
	            }
	        })
	        .success(function(data, status, headers, config) {
	            deferred.resolve(result);
	        })
	        .error(function(data, status, headers, config) {
	            deferred.resolve(result);
	        });

	        return deferred.promise;
	    };

	    this.startImportCsv = function(FileName) {
	        var deferred = $q.defer();
	        var result;
	        var params = { 'templateFileName':FileName ,
	            'dataFileName': FileName,
	            'source': 'File',
	            'entity': 'Account',
	            'feedType': 'AccountSchema'};

	        $http({
	            method: 'POST',
	            url: '/pls/cdl/import/csv',
	            headers: { 'Content-Type': 'application/json' },
	            params: params,
	        })
	        .success(function(data, status, headers, config) {
	            deferred.resolve(result);
	        })
	        .error(function(data, status, headers, config) {
	            deferred.resolve(result);
	        });

	        return deferred.promise;
	    };
});
