angular
.module('lp.create.import')
.controller('pmmlImportController', function($scope, $state, $q, ResourceUtility, StringUtility, ImportService, ImportStore) {
    var vm = this;

    angular.extend(vm, {
        importErrorMsg: '',
        accountLeadCheck: '',
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
            infoTemplate: "<h4>PMML File</h4><p>Choose a PMML File</p>",
            defaultMessage: "Example: enterprise-pmml-model.xml",
            compressed: false,
            metadataFile: true
        },
        pivotParams: {
            infoTemplate: "<h4>Pivot Mapping File</h4><p>Choose a Pivot Mapping File</p>",
            defaultMessage: "Example: pivot-mapping.txt",
            compressed: false,
            metadataFile: true
        }
    });

    vm.pmmlSelect = function(fileName) {
        vm.fileName = fileName;

        if (vm.modelDisplayName) {
            return;
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
            timestamp = year +''+ month +''+ day +'-'+ hours +''+ minutes,
            displayName = fileName.replace('.csv','').replace('.xml',''),
            displayName = displayName.substr(0, 50 - (timestamp.length + 1));

        if ((vm.modelDisplayName || '').indexOf(displayName) < 0) {
            vm.modelDisplayName = displayName + '_' + timestamp;
            vm.showNameDefault = true;
        }

        $('#modelDisplayName').focus();
        
        setTimeout(function() {
            $('#modelDisplayName').select();
        }, 1);

        $('#modelDisplayName')
            .parent('div.form-group')
            .removeClass('is-pristine');

        var timestamp = new Date().getTime();
            artifactName = vm.artifactName = fileName.replace('.csv','').replace('.xml',''),
            moduleName = vm.moduleName = artifactName + '_' + timestamp;

        vm.pmmlUploaded = false;
        vm.pmmlParams.url = vm.endpoint + moduleName + '/pmmlfiles?artifactName=' + artifactName;
        
        return vm.pmmlParams;
    }

    vm.pivotSelect = function(fileName) {
        var artifactName = vm.artifactName,
            moduleName = vm.moduleName;

        vm.pivotUploaded = false;
        vm.pivotParams.url = vm.endpoint + moduleName + '/pivotmappings?artifactName=' + artifactName;
        
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
            vm.accountLeadCheck = '';
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
        alert('WIP');
    }
    
    vm.keyupTextArea = function(event) {
        var target = event.target;
        target.style.height = 0;
        target.style.height = target.scrollHeight + 'px';
    }
});