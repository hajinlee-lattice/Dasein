module.exports = function(grunt) {
    var sourceDir = '.';
    var testDir = './test';
    // Configurable paths for the application
    var appConfig = {
        dir:  sourceDir,
        dist: 'dist',
        versionString: grunt.option('versionString') || new Date().getTime(),
        version: {
            jquery: '2.1.3',
            angular: '1.5.8',
            "angular-local-storage": '0.1.5',
            "angular-ui-router": '0.2.13',
            "angular-ui-bootstrap": '2.2.0',
            qtip2: '2.2.1',
            underscore: '1.8.2',
            webfont: '1.5.16',
            d3: '4.2.5',
            bootstrap: '3.3.4',
            "font-awesome": '4.7.0',
            "ng-prettyjson": '0.2.0',

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
                    'angular.js/<%= app.version.angular %>/angular-animate.js',
                    'angular.js/<%= app.version.angular %>/angular-sanitize.js',
                    'angular.js/<%= app.version.angular %>/angular-mocks.js',
                    'angular-local-storage/<%= app.version["angular-local-storage"] %>/angular-local-storage.js',
                    'angular-ui-router/<%= app.version["angular-ui-router"] %>/angular-ui-router.js',
                    'angular-ui-bootstrap/<%= app.version["angular-ui-bootstrap"] %>/ui-bootstrap.js',
                    'angular-ui-bootstrap/<%= app.version["angular-ui-bootstrap"] %>/ui-bootstrap-tpls.js',
                    'qtip2/<%= app.version.qtip2 %>/jquery.qtip.js',
                    'underscore.js/<%= app.version.underscore %>/underscore.js',
                    'webfont/<%= app.version.webfont %>/webfontloader.js',
                    'twitter-bootstrap/<%= app.version.bootstrap %>/js/bootstrap.js',
                    'd3/<%= app.version.d3 %>/d3.js',
                    'ng-prettyjson/<%= app.version["ng-prettyjson"] %>/ng-prettyjson.js'
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
                    'font-awesome/<%= app.version["font-awesome"] %>/css/font-awesome.css.map',
                    'ng-prettyjson/<%= app.version["ng-prettyjson"] %>/ng-prettyjson.css'
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
                    '<%= app.dist %>/assets/js/app.min.js': [
                        '<%= app.dir %>/app/le-common/**/*.js',
                        '<%= app.dir %>/app/core/util/RecursionCompiler.js',
                        '<%= app.dir %>/app/core/util/SessionUtility.js',
                        '<%= app.dir %>/app/core/directive/FileDownloaderDirective.js',
                        '<%= app.dir %>/app/core/directive/MainNavDirective.js',
                        '<%= app.dir %>/app/login/service/LoginService.js',
                        '<%= app.dir %>/app/login/controller/LoginCtrl.js',
                        '<%= app.dir %>/app/services/service/ServiceService.js',
                        '<%= app.dir %>/app/tenants/util/CamilleConfigUtility.js',
                        '<%= app.dir %>/app/tenants/util/TenantUtility.js',
                        '<%= app.dir %>/app/tenants/service/TenantService.js',
                        '<%= app.dir %>/app/tenants/directive/FeatureFlagDirective.js',
                        '<%= app.dir %>/app/tenants/directive/ObjectEntryDirective.js',
                        '<%= app.dir %>/app/tenants/directive/ListEntryDirective.js',
                        '<%= app.dir %>/app/tenants/directive/CamilleConfigDirective.js',
                        '<%= app.dir %>/app/tenants/controller/TenantListCtrl.js',
                        '<%= app.dir %>/app/tenants/controller/TenantConfigCtrl.js',
                        '<%= app.dir %>/app/modelquality/controller/ModelQualityRootCtrl.js',
                        '<%= app.dir %>/app/modelquality/directive/ModelQualityLineChart.js',
                        '<%= app.dir %>/app/modelquality/directive/ModelQualityGroupBarChart.js',
                        '<%= app.dir %>/app/modelquality/directive/MultiSelectCheckbox.js',
                        '<%= app.dir %>/app/modelquality/service/InfluxDbService.js',
                        '<%= app.dir %>/app/modelquality/service/ModelQualityService.js',
                        '<%= app.dir %>/app/modelquality/dashboard/controller/ModelQualityDashboardCtrl.js',
                        '<%= app.dir %>/app/modelquality/analyticpipeline/controller/AnalyticPipelineCtrl.js',
                        '<%= app.dir %>/app/modelquality/pipeline/controller/PipelineStepCtrl.js',
                        '<%= app.dir %>/app/modelquality/analytictest/controller/AnalyticTestCtrl.js',
                        '<%= app.dir %>/app/modelquality/publishlatest/controller/PublishLatestCtrl.js',
                        '<%= app.dir %>/app/datacloud/controller/DataCloudRootCtrl.js',
                        '<%= app.dir %>/app/datacloud/service/MetadataSrv.js',
                        '<%= app.dir %>/app/datacloud/metadata/controller/MetadataCtrl.js',
                        '<%= app.dir %>/app/datacloud/metadata2/controller/Metadata2Ctrl.js',
                        '<%= app.dir %>/app/app.js'
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
                    "<%= app.dir %>/assets/css/main.compiled.css": "<%= app.dir %>/assets/less/main.less",
                    "<%= app.dir %>/assets/css/kendo.compiled.css": "<%= app.dir %>/assets/less/kendo.less"
                }
            }
        },

        cssmin: {
            default: {
                files: {
                    '<%= app.dist %>/assets/css/main.min.css': [
                        '<%= app.dir %>/assets/css/main.compiled.css',
                        '<%= app.dir %>/assets/css/kendo.compiled.css'
                    ]
                }
            }
        },

        clean: {
            vendor: ['<%= app.dir %>/lib'],
            dist: [
                '<%= app.dist %>'
            ],
            postDist: [
                '<%= app.dir %>/assets/css/*.compiled.css'
            ]
        },

        copy: {
            instrumented: {
                files: [{
                    expand: true,
                    cwd: 'target/protractor_coverage/instrumented/',
                    src: ['**/*'],
                    dest: '/'
                }]
            },

            assets: {
                files: [{
                    expand: true,
                    cwd: '<%= app.dir %>',
                    src: ['<%= app.dir %>/assets/img/**/*'],
                    dest: '<%= app.dist %>'
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
                    '<%= app.dist %>/index.html': ['<%= app.dir %>/index.html'],
                    '<%= app.dist %>/404.html': ['<%= app.dir %>/404.html']
                }
            }
        },

        jshint: {
            options: {
                jshintrc: '.jshintrc',
                reporter: require('jshint-stylish-ex')
            },

            default: [
                'Gruntfile.js',
                '<%= app.dir %>/app/app.js',
                '<%= app.dir %>/app/**/*.js',
                '!<%= app.dir %>/app/**/*Spec.js'
            ]
        },

        watch: {
            js: {
                files: [
                    '<%= app.dir %>/Gruntfile.js',
                    '<%= app.dir %>/app/**/*.js'
                ],
                tasks: ['jshint']
            },
            css: {
                files: ['<%= app.dir %>/assets/less/**/*.less'],
                tasks: ['less:dev']
            }
        },

        // Unit tests
        karma: {
            options: {
                files:      [
                    '<%= app.dir %>/lib/js/*jquery.js',
                    '<%= app.dir %>/lib/js/*angular.js',
                    '<%= app.dir %>/lib/js/*angular-mocks.js',
                    '<%= app.dir %>/lib/js/*underscore.js',
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
                    '<%= app.dir %>/app/**/!(*Spec).js': 'coverage'
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

        concurrent: {
            options: {
                limit: 8
            },
            wget: ['wget:js', 'wget:css', 'wget:fonts', 'wget:kendojs', 'wget:kendocss', 'wget:kendofonts', 'wget:kendoimages'],
            test: ['jshint', 'karma:unit']
        },
        html2js: {
            options: {
                base: '',
                htmlmin: {
                    removeComments: true,
                    collapseWhitespace: true,
                    conservativeCollapse: true,
                    minifyCSS: true
                }
            },
            main: {
                src: ['app/**/*.html', '404.html'],
                dest: '<%= app.dist %>/assets/js/templates.js'
            },
        }

    });

    grunt.loadNpmTasks('grunt-wget');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-less');
    grunt.loadNpmTasks('grunt-contrib-cssmin');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-processhtml');
    grunt.loadNpmTasks('grunt-html2js');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-karma');
    grunt.loadNpmTasks('grunt-concurrent');

    // main task to run before deploy the dist war
    grunt.registerTask('build', [
        //'concurrent:test',
        'clean:dist',
        'jshint',
        'uglify',
        'less:dist',
        'cssmin',
        'processhtml:dist',
        'clean:postDist',
        'html2js',
        'copy:assets'
    ]);

    // download vendor javascript and css
    grunt.registerTask('init', [
        'clean:vendor',
        'concurrent:wget',
        'less:dev']);

    grunt.registerTask('unit', ['karma:unit']);

    grunt.registerTask('lint', ['jshint']);

    grunt.registerTask('lintunit', ['concurrent:test']);

    var sentryText = 'Watches for changes in any javascript file, and automatically re-runs linting and karma unit tests. If your computer can handle the strain, this should be running during active develpment';
    grunt.registerTask('sentry', sentryText, [
        'watch:css',
        'watch:js'
    ]);

};