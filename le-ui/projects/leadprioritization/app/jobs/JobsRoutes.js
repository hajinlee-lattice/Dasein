angular
    .module('pd.jobs')
    .config(['$stateProvider', function($stateProvider) {
        $stateProvider
            .state('jobs', {
                url: '/jobs',
                redirectTo: 'jobs.status'
            })
            .state('jobs.status', {
                url: '/status',
                views: {
                    "summary@": {
                        resolve: { 
                            ResourceString: function() {
                                return 'Jobs Page';
                            }
                        },
                        templateUrl: './app/navigation/summary/OneLineView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/status/StatusView.html'
                    }
                }
            })
            .state('jobs.status.ready', {
                url: '/ready/:jobId',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/table/TableView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/ready/ReadyView.html'
                    }
                }
            })
            .state('jobs.import', {
                url: '/import'
            })
            .state('jobs.import.credentials', {
                url: '/credentials',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/credentials/CredentialsView.html'
                    }
                }
            })
            .state('jobs.import.file', {
                url: '/file',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/file/FileView.html'
                    }
                }
            })
            .state('jobs.import.processing', {
                url: '/processing',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/processing/ProcessingView.html'
                    }
                }
            });
    }]);