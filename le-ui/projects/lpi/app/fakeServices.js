angular.module('lp.ratingsengine', [])
.service('RatingsEngineStore', function(){})
.service('RatingsEngineService', function(){});

angular.module('lp.segments', [])
.controller('SegmentationListController', function (){});

angular
.module('lp.segments')
.service('SegmentStore', function(){})
.service('SegmentService', function(){});


angular.module('lp.playbook', [
	'mainApp.appCommon.services.FilterService'
])
.service('PlaybookWizardStore', function(){})
.service('PlaybookWizardService', function(){});