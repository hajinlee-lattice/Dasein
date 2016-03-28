'use strict';

module.exports = function (grunt) {

    var sourceDir = 'src/main/webapp/lp2';
    // Configurable paths for the application
    var appConfig = {
        app:  sourceDir,
        env:  {
            dev:         {
                url:            'http://localhost:8081/lp2',
                apiUrl:         'http://localhost:8081',
                protractorConf:   'src/main/webapp/test/e2e/conf/protractor.conf.dev.js',
                protractorCcConf: 'src/main/webapp/test/e2e/conf/protractor.cc.conf.js'
            },
            qa:          {
                url:            'http://app.lattice.local/lp2/',
                apiUrl:         'http://app.lattice.local',
                protractorConf:   'src/main/webapp/test/e2e/conf/protractor.conf.qa.js',
                protractorCcConf: 'src/main/webapp/test/e2e/conf/protractor.cc.conf.qa.js'
            },
            prod:        {
                url:            'https://app.lattice-engines.com/lp2/',
                apiUrl:         'https://app.lattice-engines.com',
                protractorConf:   'src/main/webapp/test/e2e/conf/protractor.conf.prod.js',
                protractorCcConf: 'src/main/webapp/test/e2e/conf/protractor.cc.conf.js'
            }
        }
    };

    var env = grunt.option('env') || 'dev';
    var chosenEnv;
    if (env === 'dev') {
        chosenEnv = appConfig.env.dev;
        process.env.plstest = chosenEnv;
    } else if (env === 'qa') {
        chosenEnv = appConfig.env.qa;
    } else if (env === 'prod') {
        chosenEnv = appConfig.env.prod;
    }

    // Define the configuration for all the tasks
    grunt.initConfig({

        // Project settings
        pls:           appConfig,
        testenv:       chosenEnv,

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
                    '<%= pls.app %>/app/**/*.js',
                    'src/main/webapp/test/unit/**/*.js',
                    '<%= pls.app %>/lib/js/kendo.all.min.js'
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
                        browser: 'chrome',
                        baseUrl: '<%= testenv.url %>',
                        directConnect: true
                    }
                }
            },
            firefox:          {
                options: {
                    args: {
                        browser: 'firefox',
                        baseUrl: '<%= testenv.url %>',
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
                    url:     '<%= testenv.apiUrl %>/pls/internal/testtenants',
                    method:  'PUT',
                    headers: { MagicAuthentication: "Security through obscurity!" },
                    strictSSL: false
                }
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

        makeReport: {
            src: 'target/protractor_coverage/*.json',
            options: {
                type: 'cobertura',
                dir: 'target/protractor_coverage/reports',
                print: 'detail'
            }
        },

        concurrent: {
            test:    ['jshint:dist', 'karma:unit'],
            mac:     ['e2eChrome', 'e2eFirefox', 'e2eSafari'],
            windows: ['e2eChrome']
        }

    });

    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-karma');
    grunt.loadNpmTasks('grunt-ng-annotate');
    grunt.loadNpmTasks('grunt-protractor-runner');
    grunt.loadNpmTasks('grunt-http');
    grunt.loadNpmTasks('grunt-protractor-coverage');
    grunt.loadNpmTasks('grunt-istanbul');

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

    var e2eChromeCcText = 'Runs selenium end to end (protractor) unit tests on Chrome with code coverage';
    grunt.registerTask('e2eChromeCc', e2eChromeCcText, [
        'http:resetTenants',
        'protractor_coverage:chrome',
        'makeReport',
        'http:resetTenants'
    ]);

};