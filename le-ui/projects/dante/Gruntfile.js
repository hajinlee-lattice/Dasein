'use strict';

module.exports = function(grunt) {
    // Configurable paths for the application
    var appConfig = {
        app: '.',
        dist: '.'
    };

    // version of our software. This should really be in the package.json
    // but it gets passed in through
    var versionStringConfig = grunt.option('versionString') || '';

    // Define the configuration for all the tasks
    grunt.initConfig({
        // Project settings
        dante: appConfig,
        versionString: versionStringConfig,

        // Removes unessasary folders and files that are created during the build process
        // Force = true to allow for deleting contents outside of the grunt directory structure
        clean: {
            dist: {
                files: [
                    {
                        dot: true,
                        src: [
                            '.tmp' //,
                            //'<%= dante.dist %>/app/{,*/}*',
                            //'<%= dante.app %>/assets/templates.js'
                        ]
                    }
                ],
                options: {
                    force: true
                }
            },
            post: {
                files: [
                    {
                        dot: true,
                        src: [
                            '.tmp',
                            '<%= dante.app %>/app/*production*.js',
                            '<%= dante.app %>/assets/styles/production*.css',
                            '.sass-cache'
                        ]
                    }
                ],
                options: {
                    force: true
                }
            }
        },

        // Copies files around the directory structure. Main copies the dante website over to the
        // new distribution directory (dist). While tmpIndex copies the original index.html page
        // so we can modify the version strings without screwing up SVN. the .tmp folder gets
        // delted later on.
        copy: {
            main: {
                files: [
                    {
                        //     expand: true,
                        //     cwd:'<%= dante.app %>',
                        //     src: [
                        //         '**/*',
                        //         '!assets/styles/**/*.scss',
                        //         '!test/**',
                        //         '!assets/CommonAssets/styles/**/*.scss',
                        //         'assets/CommonAssets/styles/**/*.css',
                        //         '!**/*.js'
                        //     ],
                        //     dest: '<%= dante.dist %>/'
                        // },{
                        //     expand: true,
                        //     cwd:'<%= dante.app %>',
                        //     src: 'assets/styles/production.css',
                        //     dest: '<%= dante.dist %>/'
                        // },{
                        expand: true,
                        cwd: '<%= dante.app %>',
                        src: 'index.html',
                        dest: '<%= dante.dist %>/assets/'
                    }
                ]
            } //,
            // tmpIndex: {
            //     src: '<%= dante.app %>/index.html',
            //     dest: '.tmp/index.html'
            // }
        },

        // runs error checking on all of our (not already minified) javascript code
        jshint: {
            dist: {
                src: [
                    '<%= dante.app %>/app/**/*.js',
                    '!<%= dante.app %>/app/AppCommon/widgets/talkingPointWidget/TalkingPointParser.js',
                    '!<%= dante.app %>/app/AppCommon/vendor/**/*.js',
                    '!<%= dante.app %>/app/AppCommon/test/**/*.js'
                ],
                options: {
                    reporterOutput: '',
                    eqnull: true
                }
            }
        },

        // Unit tests
        karma: {
            options: {
                files: [
                    '<%= dante.app %>/app/AppCommon/vendor/jquery-2.1.1.js',
                    '<%= dante.app %>/app/AppCommon/vendor/angular/angular.js',
                    '<%= dante.app %>/app/AppCommon/vendor/angular/angular-mocks.js',
                    '<%= dante.app %>/app/AppCommon/test/testData/**/*.js',
                    '<%= dante.app %>/app/AppCommon/test/unit/**/*.js',
                    '<%= dante.app %>/app/**/*.js'
                ],
                frameworks: ['jasmine']
            },
            unit: {
                options: {
                    browsers: ['ChromeNoSandbox', 'Firefox', 'PhantomJS'],
                    customLaunchers: {
                        ChromeNoSandbox: {
                            base: 'Chrome',
                            flags: ['--no-sandbox']
                        }
                    },
                    singleRun: true
                }
            },
            watch: {
                options: {
                    browsers: ['PhantomJS'],
                    singleRun: false,
                    background: true,
                    autoWatch: true
                }
            },
            watchAll: {
                options: {
                    browsers: ['Chrome', 'Firefox', 'PhantomJS'],
                    singleRun: false,
                    background: false,
                    autoWatch: true
                }
            }
        },

        concat: {
            generated: {
                files: [
                    {
                        dest: '<%= dante.dist %>/assets/production.js',
                        src: [
                            '<%= dante.app %>/app/AppCommon/vendor/jquery-2.1.1.js',
                            '<%= dante.app %>/app/AppCommon/vendor/angular/angular.js',
                            '<%= dante.app %>/app/AppCommon/vendor/angular/angular-resource.js',
                            '<%= dante.app %>/app/AppCommon/vendor/angular/angular-route.js',
                            '<%= dante.app %>/app/AppCommon/vendor/angular/angular-sanitize.js',
                            '<%= dante.app %>/app/AppCommon/vendor/angular/angular-animate.js',
                            '<%= dante.app %>/app/AppCommon/vendor/jstorage.js',
                            '<%= dante.app %>/app/AppCommon/vendor/jquery-ui-1.9.2.js',
                            '<%= dante.app %>/app/AppCommon/vendor/d3.v3.js',
                            '<%= dante.app %>/app/AppCommon/vendor/moment.js',
                            '<%= dante.app %>/app/AppCommon/vendor/moment-timezone.min.js',
                            '<%= dante.app %>/app/AppCommon/vendor/angular-tooltips.js',
                            '<%= dante.app %>/app/AppCommon/vendor/angular-dropdowns.js',
                            '<%= dante.app %>/app/AppCommon/directives/ngEnterDirective.js',
                            '<%= dante.app %>/app/AppCommon/utilities/ExceptionOverrideUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/URLUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/ResourceUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/FaultUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/ConfigConstantUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/AuthenticationUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/SortUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/WidgetConfigUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/MetadataUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/AnimationUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/DateTimeFormatUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/NumberUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/AnalyticAttributeUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/StringUtility.js',
                            '<%= dante.app %>/app/AppCommon/utilities/PurchaseHistoryUtility.js',
                            '<%= dante.app %>/app/AppCommon/modals/SimpleModal.js',
                            '<%= dante.app %>/app/AppCommon/services/WidgetFrameworkService.js',
                            '<%= dante.app %>/app/AppCommon/services/PlayTileService.js',
                            '<%= dante.app %>/app/AppCommon/services/PurchaseHistoryService.js',
                            '<%= dante.app %>/app/AppCommon/services/PurchaseHistoryTooltipService.js',
                            '<%= dante.app %>/app/AppCommon/factory/ProductTreeFactory.js',
                            '<%= dante.app %>/app/AppCommon/factory/ProductTotalFactory.js',
                            '<%= dante.app %>/app/AppCommon/widgets/WidgetEventConstantUtility.js',
                            '<%= dante.app %>/app/AppCommon/widgets/screenWidget/ScreenWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/screenHeaderWidget/ScreenHeaderWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/repeaterWidget/RepeaterWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/arcChartWidget/ArcChartWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/collapsiblePanelWidget/CollapsiblePanelWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/playListTileWidget/PlayListTileWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/tabWidget/TabWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/talkingPointWidget/TalkingPointParser.js',
                            '<%= dante.app %>/app/AppCommon/widgets/talkingPointWidget/TalkingPointWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/purchaseHistoryWidget/PurchaseHistoryWidget.js',
                            '<%= dante.app %>/app/AppCommon/widgets/purchaseHistoryWidget/controllers/purchaseHistoryTimeline.js',
                            '<%= dante.app %>/app/AppCommon/widgets/purchaseHistoryWidget/controllers/purchaseHistoryNav.js',
                            '<%= dante.app %>/app/AppCommon/widgets/purchaseHistoryWidget/controllers/purchaseHistorySpendTable.js',
                            '<%= dante.app %>/app/AppCommon/widgets/purchaseHistoryWidget/controllers/purchaseHistorySegmentTable.js',
                            '<%= dante.app %>/app/AppCommon/widgets/purchaseHistoryWidget/controllers/purchaseHistoryYearTable.js',
                            '<%= dante.app %>/app/AppCommon/widgets/purchaseHistoryWidget/stores/PurchaseHistoryStore.js',
                            '<%= dante.app %>/app/AppCommon/directives/charts/ArcChartDirective.js',
                            '<%= dante.app %>/app/app.js',
                            '<%= dante.app %>/app/core/utilities/BrowserStorageUtility.js',
                            '<%= dante.app %>/app/core/utilities/ServiceErrorUtility.js',
                            '<%= dante.app %>/app/core/services/ResourceStringsService.js',
                            '<%= dante.app %>/app/core/services/SessionService.js',
                            '<%= dante.app %>/app/core/services/ConfigurationService.js',
                            '<%= dante.app %>/app/core/services/DanteWidgetService.js',
                            '<%= dante.app %>/app/core/services/NotionService.js',
                            '<%= dante.app %>/app/core/services/LpiPreviewService.js',
                            '<%= dante.app %>/app/core/controllers/MainViewController.js',
                            '<%= dante.app %>/app/core/controllers/MainHeaderController.js',
                            '<%= dante.app %>/app/core/controllers/ServiceErrorController.js',
                            '<%= dante.app %>/app/core/controllers/NoNotionController.js',
                            '<%= dante.app %>/app/core/controllers/NoAssociationController.js',
                            '<%= dante.app %>/app/plays/controllers/PlayListController.js',
                            '<%= dante.app %>/app/plays/controllers/PlayDetailsController.js',
                            '<%= dante.app %>/app/plays/controllers/NoPlaysController.js',
                            '<%= dante.app %>/app/leads/controllers/LeadDetailsController.js',
                            '<%= dante.app %>/app/purchaseHistory/controllers/PurchaseHistoryController.js',
                            '<%= dante.app %>/assets/templates.js'
                        ]
                    }
                ]
            }
        },

        // Adds Angular JS dependency injection annotations. Needed for when we minify
        // JavaScript code. For more information see: https://github.com/olov/ng-annotate
        ngAnnotate: {
            options: {
                singleQuotes: true
            },
            app: {
                files: {
                    '<%= dante.dist %>/assets/production.js': [
                        '<%= dante.dist %>/assets/production.js'
                    ]
                }
            }
        },

        uglify: {
            app: {
                files: {
                    '<%= dante.dist %>/assets/production.js': [
                        '<%= dante.dist %>/assets/production.js'
                    ]
                }
            }
        },

        // End to End (e2e) tests (aka UI automation)
        protractor: {
            options: {
                configFile: '<%= dante.app %>/test/protractor-conf.js',
                keepAlive: false // don't keep browser process alive after failures
            },
            run: {}
        },
        // sass: {
        //     options: {
        //         sourcemap: 'auto',
        //         style:     'compressed'
        //     },
        //     dist:    {
        //         files: {
        //             '<%= pls.assets %>/css/production.css': [
        //                 '<%= pls.app %>/app.component.scss'
        //             ]
        //         }
        //     },
        //     dev:     {
        //         files: {
        //             '<%= pls.assets %>/css/production.css': [
        //                 '<%= pls.app %>/app.component.scss'
        //             ]
        //         }
        //     }
        // },

        // Compiles Sass to CSS
        sass: {
            options: {
                sourcemap: 'auto',
                style: 'compressed'
            },
            dist: {
                files: {
                    '<%= dante.dist %>/assets/styles/production.css': [
                        '<%= dante.app %>/assets/styles/main.scss'
                    ]
                }
            },
            dev: {
                files: {
                    '<%= dante.dist %>/assets/styles/production.css': [
                        '<%= dante.app %>/assets/styles/main.scss'
                    ]
                }
            }
        },

        // Executes the replacement for any js/sass files in our index.html page
        usemin: {
            html: '<%= dante.dist %>/assets/index.html',
            options: {
                blockReplacements: {
                    sass: function(block) {
                        return (
                            '<link rel="stylesheet" type="text/css" href="' +
                            block.dest +
                            '">'
                        );
                    }
                }
            }
        },

        // inspects our (temporary) index.html page, and collects all the needed JS
        // files for minificaiton. Also generates the uglify and concat grunt commands
        useminPrepare: {
            html: '<%= dante.app %>/index.html',
            options: {
                dest: '<%= dante.dist %>/assets',
                flow: {
                    html: {
                        steps: {
                            js: ['concat', 'uglifyjs'],
                            css: ['concat', 'cssmin']
                        },
                        post: {}
                    }
                }
            }
        },

        // Watches for changes in the given directories. Scripts watches for javaScript
        // changes, and reruns linting / unit tests. Css watches for changes in sass files
        // and recompiles production.css
        watch: {
            scripts: {
                files: [
                    '<%= dante.app %>/app/**/*.js',
                    '<%= dante.app %>/app/app.js',
                    '<%= dante.app %>/test/**/*.js',
                    '!<%= dante.app %>/app/AppCommon/vendor/**/*.js'
                ],
                tasks: ['jshint:dist' /*, 'karma:watch:run'*/]
            },
            css: {
                files: [
                    '<%= dante.app %>/assets/styles/**/*.scss',
                    '<%= dante.app %>/assets/CommonAssets/styles/**/*.scss'
                ],
                tasks: ['sass:dev']
            },
            html: {
                files: ['<%= dante.app %>/**/*.html'],
                tasks: ['html2js']
            }
        },

        // This plugin converts a group of templates to JavaScript
        // and assembles them into an Angular module that primes the cache directly when the module is loaded.
        // html2js: {
        //     options: {
        //         module: 'templates-main',
        //         rename: function (moduleName) {
        //             return moduleName.replace('../../../Projects/DanteWebSite/', ''); // because of Gruntfile location
        //         },
        //         singleModule: true
        //     },
        //     main: {
        //         src: ['<%= dante.app %>/**/*.html'],
        //         dest: '<%= dante.app %>/assets/templates.js'
        //     }
        // }

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
                src: ['app/**/*.html'],
                dest: '<%= dante.app %>/assets/templates.js'
            }
        }
    });

    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-copy');
    //grunt.loadNpmTasks('grunt-contrib-jshint');
    //grunt.loadNpmTasks('grunt-karma');
    //grunt.loadNpmTasks('grunt-protractor-runner');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-ng-annotate');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-usemin');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-html2js');

    var defaultText =
        'The default grunt build task. Runs a full build for everything including: file linting and minification (including css), running unit tests, and versioning for production. This website ready for distribution will be placed in the <SVN Directory><Product>Projectsdist directory. This can be called just with the grunt command. The production files will then be named production_.js and production_.css.';
    grunt.registerTask('build', defaultText, [
        //'clean:dist',
        'sass:dist',
        'html2js',
        //'copy:tmpIndex',
        //'jshint:dist',
        //'karma:unit',
        'copy:main',
        'usemin',
        'concat:generated',
        'ngAnnotate:app',
        'uglify:app'
        //'clean:post'
    ]);

    var devText =
        "Compiles sass into css, and checks javascript files for errors. This needs to be run if you don't have sentry running, and you don't have a production.css file in the styles directory";
    grunt.registerTask('dev', devText, [
        'clean:post',
        'html2js',
        'jshint:dist',
        'sass:dev'
    ]);

    var e2eText = 'Runs selenium end to end (protractor) unit tests';
    grunt.registerTask('e2e', e2eText, ['protractor:run']);

    var lintText =
        "Checks all JavaScript code for possible errors. This should be run before a checkin if you aren't using grunt sentry";
    grunt.registerTask('lint', lintText, ['jshint:dist']);

    var unitText = 'Runs standard (karma) unit tests';
    grunt.registerTask('unit', unitText, ['karma:unit']);

    var sentryText =
        'Watches for changes in any javascript file, and automatically re-runs linting and karma unit tests. If your computer can handle the strain, this should be running during active develpment';
    grunt.registerTask('sentry', sentryText, [
        'karma:watch',
        'watch:css',
        'watch:scripts'
    ]);
};
