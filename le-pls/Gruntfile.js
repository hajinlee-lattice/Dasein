'use strict';

module.exports = function (grunt) {

    var sourceDir = 'src/main/webapp';
    // Configurable paths for the application
    var appConfig = {
        app:  sourceDir,
        dist: 'dist',
        env:  {
            dev:         {
                url:            'http://localhost:8080',
                protractorConf: sourceDir + '/test/e2e/conf/protractor.conf.dev.js',
                protractorCcConf: sourceDir + '/test/e2e/conf/protractor.cc.conf.js'
            },
            integration: {
                url:            'http://bodcdevhdpweb53.dev.lattice.local:8080',
                protractorConf: sourceDir + '/test/e2e/conf/protractor.conf.int.js',
                protractorCcConf: sourceDir + '/test/e2e/conf/protractor.cc.conf.int.js'
            },
            qa:          {
                url:            'http://bodcdevhdpweb52.dev.lattice.local:8080',
                protractorConf: sourceDir + '/test/e2e/conf/protractor.conf.qa.js',
                protractorCcConf: sourceDir + '/test/e2e/conf/protractor.cc.conf.qa.js'
            },
            prod:        {
                url:            'https://app.lattice-engines.com',
                protractorConf: sourceDir + '/test/e2e/conf/protractor.conf.prod.js',
                protractorCcConf: sourceDir + '/test/e2e/conf/protractor.cc.conf.js'
            }
        }
    };

    var env = grunt.option('env') || 'dev';
    var chosenEnv;
    if (env === 'dev') {
        chosenEnv = appConfig.env.dev;
        process.env.plstest = chosenEnv;
    } else if (env === 'int') {
        chosenEnv = appConfig.env.integration;
    } else if (env === 'qa') {
        chosenEnv = appConfig.env.qa;
    } else if (env === 'prod') {
        chosenEnv = appConfig.env.prod;
    }

    // version of our software. This should really be in the package.json
    // but it gets passed in through 
    var versionStringConfig = grunt.option('versionString') || '';

    // Define the configuration for all the tasks
    grunt.initConfig({

        // Project settings
        pls:           appConfig,
        testenv:       chosenEnv,
        versionString: versionStringConfig,

        // Removes unessasary folders and files that are created during the build process
        // Force = true to allow for deleting contents outside of the grunt directory structure
        clean: {
            dist: {
                files:   [{
                    dot: true,
                    src: [
                        '.tmp',
                        '<%= pls.dist %>/{,*/}*'
                    ]
                }],
                options: {
                    force: true
                }
            },
            post: {
                files:   [{
                    dot: true,
                    src: [
                        '.tmp',
                        '<%= pls.app %>/app/*production*.js',
                        '<%= pls.app %>/assets/styles/production*.css',
                        '.sass-cache'
                    ]
                }],
                options: {
                    force: true
                }
            },
            coverage: {
            	src: ['target/protractor_coverage'],
            	options: {
                    force: true
                }
            }
        },

        concurrent: {
            mac:     ['e2eChrome', 'e2eFirefox', 'e2eSafari'],
            windows: ['e2eChrome']
        },

        // Copies files around the directory structure. Main copies the pls website over to the
        // new distribution directory (dist). While tmpIndex copies the original index.html page
        // so we can modify the version strings without screwing up SVN. the .tmp folder gets
        // delted later on.
        copy: {
            main:     {
                files: [
                    {
                        expand: true,
                        cwd:    '<%= pls.app %>',
                        src:    [
                            '**/*',
                            '!assets/styles/**/*.scss',
                            '!assets/styles/**/*.map',
                            '!test/**',
                            '!assets/CommonAssets/styles/**/*.scss',
                            'assets/CommonAssets/styles/**/*.css',
                            '!**/*.js'
                        ],
                        dest:   '<%= pls.dist %>/'
                    },
                    {
                        expand: true,
                        cwd:    '<%= pls.app %>',
                        src:    'assets/styles/production_<%= versionString %>.css',
                        dest:   '<%= pls.dist %>/'
                    },
                    {expand: true, cwd: '.tmp', src: 'index.html', dest: '<%= pls.dist %>/'}
                ]
            },
            tmpIndex: {
                src:  '<%= pls.app %>/index.html',
                dest: '.tmp/index.html'
            },

            instrumented: {
                files: [{
                	expand: true,
                	cwd: 'target/protractor_coverage/instrumented/src/main/webapp/',
                    src: ['**/*'],
                    dest: 'src/main/webapp/'
                }]
            }
        },

        // runs error checking on all of our (not already minified) javascript code
        jshint: {
            dist: {
                src:     [
                    '<%= pls.app %>/app/**/*.js',
                    '!<%= pls.app %>/app/AppCommon/widgets/talkingPointWidget/TalkingPointParser.js',
                    '!<%= pls.app %>/app/AppCommon/vendor/**/*.js',
                    '!<%= pls.app %>/app/AppCommon/test/**/*.js'
                ],
                options: {
                    eqnull: true,
                    sub: true
                }
            }
        },

        // Unit tests
        karma: {
            options: {
                files:      [
                    '<%= pls.app %>/app/AppCommon/vendor/*jquery-2.1.1.js',
                    '<%= pls.app %>/app/AppCommon/vendor/angular/*angular.js',
                    '<%= pls.app %>/app/AppCommon/vendor/angular/*angular-mocks.js',
                    '<%= pls.app %>/app/AppCommon/vendor/*underscore.js',
                    '<%= pls.app %>/app/AppCommon/test/testData/**/*.js',
                    '<%= pls.app %>/app/AppCommon/test/unit/**/*.js',
                    '<%= pls.app %>/app/**/*.js'

                ],
                frameworks: ['jasmine']

            },
            unit:    {
                singleRun:     true,
                browsers:      ['PhantomJS'],
                reporters:     ['dots', 'junit', 'coverage'],
                junitReporter: {
                    outputFile: 'target/karma-test-results.xml'
                },

                preprocessors:    {
                    'src/main/webapp/**/!(angular|vendor|test)/!(*Spec).js': 'coverage'
                },
                coverageReporter: {
                    dir:       'target/jscoverage',
                    reporters: [
                        // reporters not supporting the `file` property
                        {type: 'html', subdir: 'report-html'},
                        {type: 'lcov', subdir: 'report-lcov'},
                        // reporters supporting the `file` property, use `subdir` to directly
                        // output them in the `dir` directory
                        {type: 'cobertura', subdir: '.', file: 'cobertura.xml'}
                    ]
                }

            },
            devunit: {
                options: {
                    browsers:  ['Chrome'],
                    singleRun: false
                }
            },

            watch:    {
                options: {
                    browsers:   ['PhantomJS'],
                    singleRun:  false,
                    background: true,
                    autoWatch:  true
                }
            },
            watchAll: {
                options: {
                    browsers:   ['PhantomJS'],
                    singleRun:  false,
                    background: false,
                    autoWatch:  true
                }
            }
        },

        concat: {
            generated: {
                files: [{
                    dest: '<%= pls.dist %>/app/production_<%= versionString %>.js',
                    src:  [
                        '<%= pls.app %>/app/AppCommon/vendor/jquery-2.1.1.js',
                        '<%= pls.app %>/app/AppCommon/vendor/angular/angular.js',
                        '<%= pls.app %>/app/AppCommon/vendor/angular/angular-resource.js',
                        '<%= pls.app %>/app/AppCommon/vendor/angular/angular-route.js',
                        '<%= pls.app %>/app/AppCommon/vendor/angular/angular-sanitize.js',
                        '<%= pls.app %>/app/AppCommon/vendor/ui-bootstrap-jpls-0.13.0.js',
                        '<%= pls.app %>/app/AppCommon/vendor/underscore.js',
                        '<%= pls.app %>/app/AppCommon/vendor/jstorage.js',
                        '<%= pls.app %>/app/AppCommon/vendor/d3.v3.js',
                        '<%= pls.app %>/app/AppCommon/vendor/CryptoJS.js',
                        '<%= pls.app %>/app/AppCommon/vendor/bootstrap.js',
                        '<%= pls.app %>/app/AppCommon/vendor/jquery.qtip.js',
                        '<%= pls.app %>/app/AppCommon/vendor/alasql.js',
                        '<%= pls.app %>/app/AppCommon/vendor/date.format.js',
                        '<%= pls.app %>/app/AppCommon/directives/ngEnterDirective.js',
                        '<%= pls.app %>/app/AppCommon/directives/ngQtipDirective.js',
                        '<%= pls.app %>/app/AppCommon/directives/helperMarkDirective.js',
                        '<%= pls.app %>/app/AppCommon/utilities/ExceptionOverrideUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/URLUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/ResourceUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/FaultUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/ConfigConstantUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/EvergageUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/AuthenticationUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/SortUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/WidgetConfigUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/MetadataUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/AnimationUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/DateTimeFormatUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/NumberUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/AnalyticAttributeUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/TrackingConstantsUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/StringUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/UnderscoreUtility.js',
                        '<%= pls.app %>/app/AppCommon/utilities/TimestampIntervalUtility.js',
                        '<%= pls.app %>/app/AppCommon/modals/SimpleModal.js',
                        '<%= pls.app %>/app/AppCommon/services/WidgetFrameworkService.js',
                        '<%= pls.app %>/app/AppCommon/services/PlayTileService.js',
                        '<%= pls.app %>/app/AppCommon/services/TopPredictorService.js',
                        '<%= pls.app %>/app/AppCommon/services/ThresholdExplorerService.js',
                        '<%= pls.app %>/app/AppCommon/services/ModelSummaryValidationService.js',
                        '<%= pls.app %>/app/AppCommon/widgets/WidgetEventConstantUtility.js',
                        '<%= pls.app %>/app/AppCommon/widgets/screenWidget/ScreenWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/screenHeaderWidget/ScreenHeaderWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/repeaterWidget/RepeaterWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/arcChartWidget/ArcChartWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/collapsiblePanelWidget/CollapsiblePanelWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/playListTileWidget/PlayListTileWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/tabWidget/TabWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/talkingPointWidget/TalkingPointParser.js',
                        '<%= pls.app %>/app/AppCommon/widgets/talkingPointWidget/TalkingPointWidget.js',
                        '<%= pls.app %>/app/AppCommon/directives/charts/ArcChartDirective.js',
                        '<%= pls.app %>/app/AppCommon/widgets/modelListTileWidget/ModelListTileWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/modelListCreationHistoryWidget/ModelListCreationHistoryWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/thresholdExplorerWidget/ThresholdExplorerWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/simpleTabWidget/SimpleTabWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/topPredictorWidget/TopPredictorWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/topPredictorWidget/TopPredictorAttributeWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/adminInfoWidget/AdminInfoWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/simpleGridWidget/SimpleGridWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/userManagementWidget/UserManagementWidget.js',
                        '<%= pls.app %>/app/AppCommon/widgets/leadsTabWidget/LeadsTabWidget.js',
                        '<%= pls.app %>/app/app.js',
                        '<%= pls.app %>/app/core/utilities/BrowserStorageUtility.js',
                        '<%= pls.app %>/app/core/utilities/ServiceErrorUtility.js',
                        '<%= pls.app %>/app/core/utilities/NavUtility.js',
                        '<%= pls.app %>/app/core/utilities/RightsUtility.js',
                        '<%= pls.app %>/app/core/utilities/PasswordUtility.js',
                        '<%= pls.app %>/app/core/services/HelpService.js',
                        '<%= pls.app %>/app/core/services/ResourceStringsService.js',
                        '<%= pls.app %>/app/core/services/SessionService.js',
                        '<%= pls.app %>/app/core/services/WidgetService.js',
                        '<%= pls.app %>/app/login/services/LoginService.js',
                        '<%= pls.app %>/app/config/services/ConfigService.js',
                        '<%= pls.app %>/app/models/services/ModelService.js',
                        '<%= pls.app %>/app/userManagement/services/UserManagementService.js',
                        '<%= pls.app %>/app/login/controllers/LoginController.js',
                        '<%= pls.app %>/app/login/controllers/UpdatePasswordController.js',
                        '<%= pls.app %>/app/core/controllers/MainViewController.js',
                        '<%= pls.app %>/app/core/controllers/MainHeaderController.js',
                        '<%= pls.app %>/app/config/controllers/ManageCredentialsController.js',
                        '<%= pls.app %>/app/config/modals/EnterCredentialsModal.js',
                        '<%= pls.app %>/app/userManagement/controllers/UserManagementController.js',
                        '<%= pls.app %>/app/models/controllers/ModelCreationHistoryController.js',
                        '<%= pls.app %>/app/models/controllers/MultipleModelSetupController.js',
                        '<%= pls.app %>/app/models/controllers/ModelListController.js',
                        '<%= pls.app %>/app/models/controllers/ModelDetailController.js',
                        '<%= pls.app %>/app/models/controllers/AdminInfoController.js',
                        '<%= pls.app %>/app/models/modals/DeleteModelModal.js',
                        '<%= pls.app %>/app/models/modals/StaleModelModal.js',
                        '<%= pls.app %>/app/models/modals/ImportModelModal.js',
                        '<%= pls.app %>/app/models/modals/AddSegmentModal.js',
                        '<%= pls.app %>/app/login/modals/TenantSelectionModal.js',
                        '<%= pls.app %>/app/userManagement/modals/EditUserModal.js',
                        '<%= pls.app %>/app/userManagement/modals/AddUserModal.js',
                        '<%= pls.app %>/app/userManagement/modals/DeleteUserModal.js'

                    ]
                }]
            }
        },

        // Adds Angular JS dependency injection annotations. Needed for when we minify
        // JavaScript code. For more information see: https://github.com/olov/ng-annotate
        ngAnnotate: {
            options: {
                singleQuotes: true
            },
            app:     {
                files: {
                    '<%= pls.dist %>/app/production_<%= versionString %>.js': ['<%= pls.dist %>/app/production_<%= versionString %>.js']
                }
            }
        },

        uglify: {
            app: {
                files: {
                    '<%= pls.dist %>/app/production_<%= versionString %>.js': ['<%= pls.dist %>/app/production_<%= versionString %>.js']
                }
            }
        },

        // End to End (e2e) tests (aka UI automation)
        protractor: {
            options:          {
                configFile: '<%= testenv.protractorConf %>',
                noColor:    false,
                keepAlive:  false // don't keep browser process alive after failures
            },
            chrome:           {
                options: {
                    args: {
                        browser:       'chrome',
                        baseUrl:       '<%= testenv.url %>',
                        directConnect: true
                    }
                }
            },
            firefox:          {
                options: {
                    args: {
                        browser:       'firefox',
                        baseUrl:       '<%= testenv.url %>',
                        directConnect: true
                    }
                }
            },
            internetexplorer: {
                options: {
                    args: {
                        browser: 'internet explorer',
                        baseUrl: '<%= testenv.url %>'
                    }
                }
            },
            safari:           {
                options: {
                    args: {
                        browser: 'safari',
                        baseUrl: '<%= testenv.url %>'
                    }
                }
            }
        },

        http: {
            resetTenants: {
                options: {
                    url:     '<%= testenv.url %>/pls/internal/testtenants',
                    method:  'PUT',
                    headers: { MagicAuthentication: "Security through obscurity!" }
                }
            }

        },

        rename: {
            moveAppToBak: {
                src:  '<%= pls.app %>',
                dest: '<%= pls.app %>-bak'
            },

            moveDistToApp: {
                src:  '<%= pls.dist %>',
                dest: '<%= pls.app %>'
            }
        },

        // Find all instances of @@versionString in our index.html page and replace
        // them with the passed in version string (defaults to '')
        replace: {
            dist: {
                options: {
                    patterns: [
                        {
                            match:       'versionString',
                            replacement: '<%= versionString %>'
                        }
                    ]
                },
                files:   {
                    '.tmp/index.html': '.tmp/index.html'
                }
            }
        },

        // Compiles Sass to CSS
        sass: {
            options: {
                sourcemap: 'none',
                style:     'compressed'
            },
            dist:    {
                files: {
                    '<%= pls.app %>/assets/styles/production_<%= versionString %>.css': '<%= pls.app %>/assets/styles/main.scss'
                }
            },
            dev:     {
                files: {
                    '<%= pls.app %>/assets/styles/production.css': '<%= pls.app %>/assets/styles/main.scss'
                }
            }
        },

        // Executes the replacement for any js/sass files in our index.html page
        usemin: {
            html:    '<%= pls.dist %>/index.html',
            options: {
                blockReplacements: {
                    sass: function (block) {
                        return '<link rel="stylesheet" href="' + block.dest + '">';
                    }
                }
            }
        },

        // inspects our (temporary) index.html page, and collects all the needed JS
        // files for minificaiton. Also generates the uglify and concat grunt commands
        useminPrepare: {
            html:    '.tmp/index.html',
            options: {
                dest: '<%= pls.dist %>',
                flow: {
                    html: {
                        steps: {
                            js:  ['concat', 'uglifyjs'],
                            css: ['concat', 'cssmin']
                        },
                        post:  {}
                    }
                }
            }
        },

        // Watches for changes in the given directories. Scripts watches for javaScript
        // changes, and reruns linting / unit tests. Css watches for changes in sass files
        // and recompiles production.css
        watch: {
            scripts: {
                files: ['<%= pls.app %>/app/**/*.js',
                    '<%= pls.app %>/app/app.js',
                    '<%= pls.app %>/test/**/*.js',
                    '!<%= pls.app %>/app/AppCommon/vendor/**/*.js'],
                tasks: ['jshint:dist', 'karma:watch:run']
            },
            css:     {
                files: ['<%= pls.app %>/assets/styles/**/*.scss'],
                tasks: ['sass:dev']
            }
        },
        
        
        
        // E2E UI Automation with code coverage
        protractor_coverage: {
            options: {
                keepAlive: false,
                noColor: false,
                coverageDir: 'target/protractor_coverage',
                configFile: '<%= testenv.protractorCcConf %>'
            },
            chrome:           {
                options: {
                    args: {
                        browser:       'chrome',
                        baseUrl:       '<%= testenv.url %>'
                    }
                }
            },
            run: {}
        },

        instrument: {
            files: 'src/main/webapp/app/**/**[!vendor]/*[!Spec].js',
            options: {
            	lazy: true,
                basePath: "target/protractor_coverage/instrumented"
            }
        },

        makeReport: {
            src: 'target/protractor_coverage/*.json',
            options: {
                type: 'cobertura',
                dir: 'target/protractor_coverage/reports',
                print: 'detail'
            }
        }

    });
    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-karma');
    grunt.loadNpmTasks('grunt-ng-annotate');
    grunt.loadNpmTasks('grunt-protractor-runner');
    grunt.loadNpmTasks('grunt-usemin');
    grunt.loadNpmTasks('grunt-rename');
    grunt.loadNpmTasks('grunt-replace');
    grunt.loadNpmTasks('grunt-http');
    grunt.loadNpmTasks('grunt-protractor-coverage');
    grunt.loadNpmTasks('grunt-istanbul');

    var defaultText = 'The default grunt build task. Runs a full build for everything including: file linting and minification (including css), running unit tests, and versioning for production. This website ready for distribution will be placed in the <SVN Directory>\<Product>\Projects\dist directory. This can be called just with the grunt command. The production files will then be named production_.js and production_.css.';
    grunt.registerTask('default', defaultText, [
        'clean:dist',
        'copy:tmpIndex',
        'replace',
        'jshint:dist',
        'karma:unit',
        //'protractor:run',
        'concat:generated',
        'ngAnnotate:app',
        'uglify:app',
        'sass:dev',
        'sass:dist',
        'copy:main',
        'usemin',
        'clean:post'
    ]);

    var prepWarText = 'Move dist into webapp directory for maven webapp packager to pick up.  This should only be run on build machine because it messes with the source webapp.';
    grunt.registerTask('prepwar', prepWarText, [
        'rename:moveDistToApp',
        'rename:moveAppToBak'
    ]);

    var devText = 'Compiles sass into css, and checks javascript files for errors. This needs to be run if you don\'t have sentry running, and you don\'t have a production.css file in the styles directory';
    grunt.registerTask('dev', devText, [
        'clean:post',
        'jshint:dist',
        'sass:dev'
    ]);

    var e2eChromeText = 'Runs selenium end to end (protractor) unit tests on Chrome';
    grunt.registerTask('e2eChrome', e2eChromeText, [
        'http:resetTenants',
        'protractor:chrome',
        'http:resetTenants'
    ]);

    var e2eFirefoxText = 'Runs selenium end to end (protractor) unit tests on Firefox';
    grunt.registerTask('e2eFirefox', e2eFirefoxText, [
        'http:resetTenants',
        'protractor:firefox',
        'http:resetTenants'
    ]);

    var e2eInternetExplorerText = 'Runs selenium end to end (protractor) unit tests on Internet Explorer';
    grunt.registerTask('e2eInternetExplorer', e2eInternetExplorerText, [
        'http:resetTenants',
        'protractor:internetexplorer',
        'http:resetTenants'
    ]);

    var e2eSafariText = 'Runs selenium end to end (protractor) unit tests on Safari';
    grunt.registerTask('e2eSafari', e2eSafariText, [
        'http:resetTenants',
        'protractor:safari',
        'http:resetTenants'
    ]);

    var e2eMacText = 'Runs selenium end to end (protractor) Mac tests';
    grunt.registerTask('e2eMac', e2eMacText, [
        'http:cleanupUsers',
        'concurrent:mac',
        'http:cleanupUsers'
    ]);

    var e2eWinText = 'Runs selenium end to end (protractor) Windows tests';
    grunt.registerTask('e2eWin', e2eWinText, [
        'http:resetTenants',
        'concurrent:windows',
        'http:resetTenants'
    ]);

    var lintText = 'Checks all JavaScript code for possible errors. This should be run before a checkin if you aren\'t using grunt sentry';
    grunt.registerTask('lint', lintText, [
        'jshint:dist'
    ]);

    var unitText = 'Runs standard (karma) unit tests';
    grunt.registerTask('unit', unitText, [
        'karma:unit'
    ]);

    var devUnitText = 'Runs standard (karma) unit tests in Chrome for debugging purposes';
    grunt.registerTask('devunit', devUnitText, [
        'karma:devunit'
    ]);

    var sentryText = 'Watches for changes in any javascript file, and automatically re-runs linting and karma unit tests. If your computer can handle the strain, this should be running during active develpment';
    grunt.registerTask('sentry', sentryText, [
        'karma:watch',
        'watch:scripts',
        'watch:css'
    ]);

    var e2eChromeCcText = 'Runs selenium end to end (protractor) unit tests on Chrome with code coverage';
    grunt.registerTask('e2eChromeCc', e2eChromeCcText, [
        'http:resetTenants',
        'protractor_coverage:chrome',
        'makeReport',
        'http:resetTenants'
    ]);
    
    var instrumentJsText = 'Instrument javascript code for code coverage';
    grunt.registerTask('instrumentJs', instrumentJsText, [
        'instrument',
        'copy:instrumented'
    ]);

};