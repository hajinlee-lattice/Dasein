angular
.module('lp.create.import')
.controller('pmmlImportController', function($scope, $state, $q, ResourceUtility, StringUtility, ImportService, ImportStore, CancelJobModal) {
    var vm = this;

    angular.extend(vm, {
        importErrorMsg: '',
        accountLeadCheck: 'SalesforceLead',
        modelDisplayName: '',
        modelDescription: '',
        moduleName: '',
        showTypeDefault: false,
        showNameDefault: false,
        showImportError: false,
        showImportSuccess: false,
        pmmlUploaded: false,
        pivotUploaded: false,
        ResourceUtility: ResourceUtility,
        endpoint: '/pls/metadatauploads/modules/',
        pmmlParams: {
            infoTemplate: "<p class='template-description'>Upload a PMML model file.</p>",
            defaultMessage: "Example: enterprise-pmml-model.xml",
            compressed: true,
            metadataFile: true,
            tooltipConfiguration: {
                tooltipSize: 'small',
                whiteFont: true
            }
        },
        pivotParams: {
            infoTemplate: "<h4>Pivot Mapping File</h4><p>Choose a Pivot Mapping File</p>",
            defaultMessage: "Example: pivot-mapping.txt",
            compressed: false,
            metadataFile: true
        }
    });


    vm.pmmlSelect = function(fileName) {
        vm.fileName = vm.pmmlFileName = fileName;

        var fileName = vm.sanitize(fileName),
            timestamp = new Date().getTime(),
            artifactName = vm.artifactName = vm.stripExt(fileName),
            moduleName = vm.moduleName = artifactName + '_' + timestamp;

        vm.pmmlUploaded = false;
        vm.pmmlParams.url = vm.endpoint + moduleName + '/pmmlfiles?artifactName=' + artifactName;

        if (vm.modelDisplayName) {
            return vm.pmmlParams;
        }

        var date = new Date(),
            day = date.getDate(),
            year = date.getFullYear(),
            month = (date.getMonth() + 1),
            seconds = date.getSeconds(),
            minutes = date.getMinutes(),
            hours = date.getHours(),
            month = (month < 10 ? '0' + month : month),
            day = (day < 10 ? '0' + day : day),
            minutes = (minutes < 10 ? '0' + minutes : minutes),
            hours = (hours < 10 ? '0' + hours : hours),
            timestamp = year +''+ month +''+ day +'-'+ hours +''+ minutes + '-' + seconds,
            displayName = fileName.replace('.csv','').replace('.xml',''),
            displayName = displayName.substr(0, 50 - (timestamp.length + 1));

        if ((vm.modelDisplayName || '').indexOf(displayName) < 0) {
            vm.modelDisplayName = displayName + '_' + timestamp;
            vm.showNameDefault = true;
        }

        var modelDisplayNameEl = $('#modelDisplayName')
        modelDisplayNameEl.focus();

        setTimeout(function() {
            modelDisplayNameEl.select();
        }, 1);

        modelDisplayNameEl
            .parent('div.form-group')
            .removeClass('is-pristine');

        return vm.pmmlParams;
    }

    vm.pivotSelect = function(fileName) {
        var artifactName = vm.sanitize(fileName),
            pivotFile = vm.pivotFileName = artifactName,
            moduleName = vm.moduleName;

        vm.pivotUploaded = false;
        vm.pivotParams.url = vm.endpoint + moduleName + '/pivotmappings?artifactName=' + vm.stripExt(artifactName);
        
        return vm.pivotParams;
    }

    vm.pmmlLoad = function(headers) {

    }
    
    vm.pmmlDone = function() {
        vm.pmmlUploaded = true;
    }
    
    vm.pivotDone = function() {
        vm.pivotUploaded = true;
    }

    vm.pmmlCancel = function() {
        vm.pivotParams.scope.cancel();
        vm.pivotUploaded = false;
        vm.moduleName = '';

        if (vm.showTypeDefault) {
            vm.showTypeDefault = false;
            vm.accountLeadCheck = 'SalesforceLead';
        }

        if (vm.showNameDefault) {
            vm.showNameDefault = false;
            vm.modelDisplayName = '';
        }

        var xhr = ImportStore.Get('cancelXHR', true);
        
        if (xhr) {
            xhr.abort();
        }
    }
    
    vm.pivotCancel = function() {
    
    }
    
    vm.changeType = function() {
        vm.showTypeDefault = false;
    }
    
    vm.changeName = function() {
        vm.showNameDefault = false;
    }
    
    vm.clickUpload = function() {
        vm.showImportError = false;
        vm.importErrorMsg = "";
    }
    
    vm.clickNext = function() {
        ShowSpinner('Modeling...');

        var options = {
                displayname: this.modelDisplayName,
                modelName: vm.sanitize(this.modelDisplayName),
                schema: vm.accountLeadCheck,
                module: this.moduleName,
                pmmlfile: vm.stripExt(vm.sanitize(this.pmmlFileName)) + '.xml'
            };

        if (this.pivotFileName) {
            options.pivotfile = vm.stripExt(vm.sanitize(this.pivotFileName)) + '.csv';
        }

        ImportService.StartPMMLModeling(options).then(function(result) {
            if (result.Result && result.Result != "") {
                setTimeout(function() {
                    $state.go('home.models.pmml.job', { applicationId: result.Result });
                }, 1);
            }
        });
    }
    
    vm.keyupTextArea = function(event) {
        var target = event.target;
        target.style.height = 0;
        target.style.height = target.scrollHeight + 'px';
    }

    vm.cancelJob = function($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        CancelJobModal.show(null, {sref:'home.models'});
    }

    vm.sanitize = function(fileName) {
        return fileName.replace(/[^A-Za-z0-9_\.]/g,'_');
    }

    vm.stripExt = function(fileName) {
        var fnSplit = (fileName || '').split('.');

        if (fnSplit.length > 1) {
            fnSplit.pop();
        }

        return fnSplit.join('.');
    }
});