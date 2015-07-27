module.exports = function(grunt) {
    var sourceDir = 'src/main/webapp';
    var testDir = 'src/test/webapp';
    // Configurable paths for the application
    var appConfig = {
        dir:  sourceDir,
        dist: 'dist',
        www: 'www',
        distDir: 'dist/' + sourceDir,
        leCommon: '../le-common/npm/release',
        versionString: new Date().getTime(),
        version: {
            "le-common": '0.0.1',

            jquery: '2.1.3',
            angular: '1.3.15',
            "angular-local-storage": '0.1.5',
            "angular-ui-router": '0.2.13',
            "angular-ui-bootstrap": '0.12.1',
            qtip2: '2.2.1',
            underscore: '1.8.2',
            webfont: '1.5.16',

            bootstrap: '3.3.4',
            "font-awesome": '4.3.0',

            kendo: '2015.1.408'
        },
        env:  {
            dev:         {
                url:            'http://localhost:8085',
                protractorConf: testDir + '/e2e/conf/protractor.conf.js',
                protractorCcConf: testDir + '/e2e/conf/protractorCc.conf.js'
            },
            integration: {
                url:            'http://bodcdevvjty21.dev.lattice.local:8080',
                protractorConf: testDir + '/e2e/conf/protractor.conf.js',
                protractorCcConf: testDir + '/e2e/conf/protractorCc.conf.js'
            },
            qa:          {
                url:            'http://bodcdevvjty20.dev.lattice.local:8080',
                protractorConf: testDir + '/e2e/conf/protractor.conf.js',
                protractorCcConf: testDir + '/e2e/conf/protractorCc.conf.js'
            },
            prod:        {
                url:            'http://admin.lattice.local',
                protractorConf: testDir + '/e2e/conf/protractor.conf.js',
                protractorCcConf: testDir + '/e2e/conf/protractorCc.conf.js'
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

    grunt.initConfig({
        app:     appConfig,
        testenv: chosenEnv,

        wget: {
            // download un-minimized version of vendor javascript libraries from CDN
            js: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'jquery/<%= app.version.jquery %>/jquery.js',
                    'angular.js/<%= app.version.angular %>/angular.js',
                    'angular.js/<%= app.version.angular %>/angular-sanitize.js',
                    'angular.js/<%= app.version.angular %>/angular-mocks.js',
                    'angular-local-storage/<%= app.version["angular-local-storage"] %>/angular-local-storage.js',
                    'angular-ui-router/<%= app.version["angular-ui-router"] %>/angular-ui-router.js',
                    'angular-ui-bootstrap/<%= app.version["angular-ui-bootstrap"] %>/ui-bootstrap.js',
                    'angular-ui-bootstrap/<%= app.version["angular-ui-bootstrap"] %>/ui-bootstrap-tpls.js',
                    'qtip2/<%= app.version.qtip2 %>/jquery.qtip.js',
                    'underscore.js/<%= app.version.underscore %>/underscore.js',
                    'webfont/<%= app.version.webfont %>/webfontloader.js',
                    'twitter-bootstrap/<%= app.version.bootstrap %>/js/bootstrap.js'
                ],
                dest: '<%= app.dir %>/lib/js'
            },

            css: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'bootswatch/<%= app.version.bootstrap %>/simplex/bootstrap.css',
                    'qtip2/<%= app.version.qtip2 %>/jquery.qtip.css',
                    'font-awesome/<%= app.version["font-awesome"] %>/css/font-awesome.css',
                    'font-awesome/<%= app.version["font-awesome"] %>/css/font-awesome.css.map'
                ],
                dest: '<%= app.dir %>/lib/css'
            },

            fonts: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'font-awesome/<%= app.version["font-awesome"] %>/fonts/FontAwesome.otf',
                    'font-awesome/<%= app.version["font-awesome"] %>/fonts/fontawesome-webfont.eot',
                    'font-awesome/<%= app.version["font-awesome"] %>/fonts/fontawesome-webfont.svg',
                    'font-awesome/<%= app.version["font-awesome"] %>/fonts/fontawesome-webfont.ttf',
                    'font-awesome/<%= app.version["font-awesome"] %>/fonts/fontawesome-webfont.woff',
                    'font-awesome/<%= app.version["font-awesome"] %>/fonts/fontawesome-webfont.woff2'
                ],
                dest: '<%= app.dir %>/lib/fonts'
            },

            kendojs: {
                options: {
                    baseUrl: 'http://cdn.kendostatic.com/<%= app.version.kendo %>/js/'
                },
                src: ['kendo.all.min.js', 'kendo.all.min.js'],
                dest: '<%= app.dir %>/lib/js'
            },

            kendocss: {
                options: {
                    baseUrl: 'http://cdn.kendostatic.com/<%= app.version.kendo %>/styles/'
                },
                src: [
                    'kendo.common-bootstrap.min.css',
                    'kendo.bootstrap.min.css',
                    'kendo.dataviz.min.css',
                    'kendo.dataviz.bootstrap.min.css',
                    'kendo.mobile.all.min.css'
                ],
                dest: '<%= app.dir %>/lib/css'
            },

            kendofonts: {
                options: {
                    baseUrl: 'http://cdn.kendostatic.com/<%= app.version.kendo %>/styles/images/'
                },
                src: ['kendoui.woff', 'kendoui.woff'],
                dest: '<%= app.dir %>/lib/css/images'
            },

            kendoimages: {
                options: {
                    baseUrl: 'http://cdn.kendostatic.com/<%= app.version.kendo %>/styles/'
                },
                src: ['Bootstrap/sprite.png', 'Bootstrap/loading-image.gif'],
                dest: '<%= app.dir %>/lib/css/Bootstrap'
            }
        },

        uglify: {
            options: {
                mangle: false
            },
            // the order of source files matters!
            default: {
                files: {
                    '<%= app.dir %>/assets/js/app.min.js': [
                        '<%= app.distDir %>/lib/js/le-common.js',
                        '<%= app.distDir %>/app/core/util/RecursionCompiler.js',
                        '<%= app.distDir %>/app/core/util/SessionUtility.js',
                        '<%= app.distDir %>/app/core/directive/FileDownloaderDirective.js',
                        '<%= app.distDir %>/app/core/directive/MainNavDirective.js',
                        '<%= app.distDir %>/app/login/service/LoginService.js',
                        '<%= app.distDir %>/app/login/controller/LoginCtrl.js',
                        '<%= app.distDir %>/app/services/service/ServiceService.js',
                        '<%= app.distDir %>/app/tenants/util/CamilleConfigUtility.js',
                        '<%= app.distDir %>/app/tenants/util/TenantUtility.js',
                        '<%= app.distDir %>/app/tenants/service/TenantService.js',
                        '<%= app.distDir %>/app/tenants/directive/FeatureFlagDirective.js',
                        '<%= app.distDir %>/app/tenants/directive/ObjectEntryDirective.js',
                        '<%= app.distDir %>/app/tenants/directive/ListEntryDirective.js',
                        '<%= app.distDir %>/app/tenants/directive/CamilleConfigDirective.js',
                        '<%= app.distDir %>/app/tenants/controller/TenantListCtrl.js',
                        '<%= app.distDir %>/app/tenants/controller/TenantConfigCtrl.js',
                        '<%= app.distDir %>/app/app.js'
                    ]
                }
            }
        },

        less: {
            dev: {
                files: {
                    "<%= app.dir %>/assets/css/main.compiled.css": "<%= app.dir %>/assets/less/main.less",
                    "<%= app.dir %>/assets/css/kendo.compiled.css": "<%= app.dir %>/assets/less/kendo.less"
                }
            },
            dist: {
                files: {
                    "<%= app.dir %>/assets/css/main.compiled.css": "<%= app.distDir %>/assets/less/main.less",
                    "<%= app.dir %>/assets/css/kendo.compiled.css": "<%= app.distDir %>/assets/less/kendo.less"
                }
            }
        },

        cssmin: {
            default: {
                files: {
                    '<%= app.dir %>/assets/css/main.min.css': [
                        '<%= app.dir %>/assets/css/main.compiled.css',
                        '<%= app.dir %>/assets/css/kendo.compiled.css'
                    ]
                }
            }
        },

        clean: {
            vendor: ['<%= app.dir %>/lib'],
            dist: [
                '<%= app.dir %>/*.html',
                '<%= app.dir %>/app/**/*.js',
                '<%= app.dir %>/lib',
                '<%= app.dir %>/assets/less'
            ],
            postDist: [
                '<%= app.dir %>/assets/css/*.compiled.css'
            ],
            www: [
                '<%= app.dir %>/app',
                '<%= app.dir %>/assets',
                '<%= app.dir %>/*.html'
            ],
            restore: [
                '<%= app.dir %>/assets/js',
                '<%= app.dir %>/assets/css/main_*.min.css',
                '<%= app.dist %>'
            ]
        },

        /**
         * lecommon moves le-common.js from le-common project to here.
         * backup moves files to dist folder to app folder
         * restore moves files from dist folder back to app folder
         * */
        copy: {
            lecommon: {
                expand: true,
                cwd: '<%= app.leCommon %>/<%= app.version["le-common"] %>',
                src: ['le-common.js'],
                dest: '<%= app.dir %>/lib/js'
            },
            backup: {
                expand: true,
                src: [
                    '<%= app.dir %>/*.html',
                    '<%= app.dir %>/app/**/*.js',
                    '<%= app.dir %>/lib/js/le-common.js',
                    '<%= app.dir %>/assets/less/*'
                ],
                dest: '<%= app.dist %>',
                filter: 'isFile'
            },
            www: {
                expand: true,
                cwd: '<%= app.dir %>',
                src: '**/*',
                dest: '<%= app.www %>'
            },
            restorewww: {
                expand: true,
                cwd: '<%= app.www %>',
                src: '**/*',
                dest: '<%= app.dir %>'
            },
            restore: {
                expand: true,
                cwd: '<%= app.dist %>/<%= app.dir %>',
                src: '**/*',
                dest: '<%= app.dir %>'
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

        processhtml: {
            // redirect vendor javascript/css to minimized version on CDN
            options: {
                data: {
                    version: appConfig.version,
                    versionString: appConfig.versionString
                }
            },

            dist: {
                files: {
                    '<%= app.dir %>/index.html': ['<%= app.distDir %>/index.html'],
                    '<%= app.dir %>/404.html': ['<%= app.distDir %>/404.html']
                }
            }
        },

        jshint: {
            options: {
                reporter: require('jshint-stylish-ex')
            },

            default: [
                'Gruntfile.js',
                '<%= app.dir %>/app/app.js',
                '<%= app.dir %>/app/**/*.js',
                '!<%= app.dir %>/app/**/*Spec.js'
            ]
        },

        // Unit tests
        karma: {
            options: {
                files:      [
                    '<%= app.dir %>/lib/js/*jquery.js',
                    '<%= app.dir %>/lib/js/*angular.js',
                    '<%= app.dir %>/lib/js/*angular-mocks.js',
                    '<%= app.dir %>/lib/js/*underscore.js',
                    '<%= app.dir %>/lib/js/*le-common.js',
                    '<%= app.dir %>/**/*.js'
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
                    'src/main/webapp/app/**/!(*Spec).js': 'coverage'
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
                    browsers:  ['PhantomJS'],
                    singleRun: true
                },
                reporters:     ['dots', 'junit', 'coverage'],
                junitReporter: {
                    outputFile: 'target/karma-test-results.xml'
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
                        browser: 'chrome',
                        baseUrl: '<%= testenv.url %>'
                    }
                }
            }
        },

        instrument: {
            files: 'src/main/webapp/app/**/*[!Spec].js',
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
        },

        concurrent: {
            wget: ['wget:js', 'wget:css', 'wget:fonts', 'wget:kendojs', 'wget:kendocss', 'wget:kendofonts', 'wget:kendoimages'],
            test: ['jshint', 'karma:unit']
        }

    });

    grunt.loadNpmTasks('grunt-wget');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-less');
    grunt.loadNpmTasks('grunt-contrib-cssmin');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-processhtml');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-karma');
    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-protractor-runner');
    grunt.loadNpmTasks('grunt-protractor-coverage');
    grunt.loadNpmTasks('grunt-istanbul');

    // main task to run before deploy the dist war
    grunt.registerTask('dist', [
        'concurrent:test',
        'copy:backup',
        'clean:dist',
        'uglify',
        'less:dist',
        'cssmin',
        'processhtml:dist',
        'clean:postDist'
    ]);

    // download vendor javascript and css
    grunt.registerTask('init', [
        'clean:vendor',
        'copy:lecommon',
        'concurrent:wget',
        'less:dev']);

    var instrumentJsText = 'Instrument javascript code for code coverage';
    grunt.registerTask('instrumentJs', instrumentJsText, [
        'instrument',
        'copy:instrumented'
    ]);

    grunt.registerTask('www', ['copy:www', 'clean:www']);

    grunt.registerTask('unit', ['karma:unit']);

    grunt.registerTask('lint', ['jshint']);

    grunt.registerTask('lintunit', ['concurrent:test']);

    // restore dev setup after run a default task
    grunt.registerTask('restore', ['copy:restorewww','copy:restore', 'clean:restore', 'less:dev']);

    var e2eChromeText = 'Runs selenium end to end (protractor) unit tests on Chrome';
    grunt.registerTask('e2eChrome', e2eChromeText, ['protractor:chrome']);

    var e2eFirefoxText = 'Runs selenium end to end (protractor) unit tests on Firefox';
    grunt.registerTask('e2eFirefox', e2eFirefoxText, ['protractor:firefox']);

    var e2eInternetExplorerText = 'Runs selenium end to end (protractor) unit tests on Internet Explorer';
    grunt.registerTask('e2eInternetExplorer', e2eInternetExplorerText, ['protractor:internetexplorer']);

    var e2eSafariText = 'Runs selenium end to end (protractor) unit tests on Safari';
    grunt.registerTask('e2eSafari', e2eSafariText, ['protractor:safari']);

    var e2eMacText = 'Runs selenium end to end (protractor) Mac tests';
    grunt.registerTask('e2eMac', e2eMacText, ['concurrent:mac']);

    var e2eWinText = 'Runs selenium end to end (protractor) Windows tests';
    grunt.registerTask('e2eWin', e2eWinText, ['concurrent:windows']);

    var e2eChromeCcText = 'Runs selenium end to end (protractor) unit tests on Chrome with code coverage';
    grunt.registerTask('e2eChromeCc', e2eChromeCcText, [
        'protractor_coverage:chrome',
        'makeReport'
    ]);

};