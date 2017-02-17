'use strict';

module.exports = function (grunt) {

    var sourceDir = './';
    // Configurable paths for the application
    var appConfig = {
        app:  sourceDir,
        dist: 'assets',
        env:  {
            dev:         {
                url:            'http://localhost:3000',
                apiUrl:         'http://localhost:8081',
                protractorConf:   sourceDir + 'test/e2e/conf/protractor.conf.dev.js',
                protractorCcConf: sourceDir + 'test/e2e/conf/protractor.cc.conf.js'
            },
            qa:          {
                url:            'http://app2.lattice.local',
                apiUrl:         'http://app2.lattice.local',
                protractorConf:   sourceDir + 'test/e2e/conf/protractor.conf.qa.js',
                protractorCcConf: sourceDir + 'test/e2e/conf/protractor.cc.conf.qa.js'
            },
            prod:        {
                url:            'https://app.lattice-engines.com',
                apiUrl:         'https://app.lattice-engines.com',
                protractorConf:   sourceDir + 'test/e2e/conf/protractor.conf.prod.js',
                protractorCcConf: sourceDir + 'test/e2e/conf/protractor.cc.conf.js'
            }
        }
    };

    // version of our software. This should really be in the package.json
    // but it gets passed in through 
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();
    var env = grunt.option('env') || 'dev';
    var chosenEnv;
    if (env === 'dev') {
        chosenEnv = appConfig.env.dev;
    } else if (env === 'qa') {
        chosenEnv = appConfig.env.qa;
    } else if (env === 'prod') {
        chosenEnv = appConfig.env.prod;
    }

    // Define the configuration for all the tasks
    grunt.initConfig({

        // Project settings
        pls:           appConfig,
        versionString: versionStringConfig,
        testenv:       chosenEnv,

        // Removes unessasary folders and files that are created during the build process
        // Force = true to allow for deleting contents outside of the grunt directory structure
        clean: {
            lib: {
                files:   [{
                    dot: true,
                    src: ['<%= pls.app %>/../common/lib']
                }],
                options: {
                    force: true
                }
            },
            dist: {
                files:   [{
                    dot: true,
                    src: [
                        '.tmp',
                        '<%= pls.app %>/app/production_*.js',
                        '<%= pls.app %>/assets/styles/production_*.css'
                    ]
                }],
                options: {
                    force: true
                }
            },
            post: {
                files:   [{
                    dot: true,
                    src: [ '.tmp' ]
                }],
                options: {
                    force: true
                }
            },
            build: {
                files:   [{
                    dot: true,
                    src: [ 'dist/*.*' ]
                }],
                options: {
                    force: true
                }
            },
            jsAndCss: {
                files:   [{
                    dot: true,
                    src: [
                        '<%= pls.app %>/**/*.js',
                        '<%= pls.app %>/**/*.css',
                        '<%= pls.app %>/**/*.scss',
                        '!<%= pls.app %>/lib/js/*.js',
                        '!<%= pls.app %>/lib/css/*.css',
                        '!<%= pls.app %>/app/*.min.js',
                        '!<%= pls.app %>/assets/styles/*.min.css'
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

        // Compiles Sass to CSS
        sass: {
            options: {
                sourcemap: 'auto',
                style:     'compressed'
            },
            dist:    {
                files: {
                    '<%= pls.app %>/assets/styles/production_<%= versionString %>.min.css': '<%= pls.app %>/assets/styles/main.scss'
                }
            },
            dev:     {
                files: {
                    '<%= pls.app %>/assets/styles/production.css': [
                        '<%= pls.app %>/assets/styles/main.scss'
                    ]
                }
            }
        },

        // runs error checking on all of our (not already minified) javascript code
        jshint: {
            dist: {
                src:     [
                    '<%= pls.app %>/app/**/*.js',
                    '!<%= pls.app %>/../common/lib/js/**/*.js',
                    '!<%= pls.app %>/app/AppCommon/widgets/talkingPointWidget/TalkingPointParser.js',
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
                    '<%= pls.app %>/../common/lib/js/*jquery.js',
                    '<%= pls.app %>/../common/lib/js/*jstorage.js',
                    '<%= pls.app %>/../common/lib/js/angular/*angular.js',
                    '<%= pls.app %>/../common/lib/js/angular/*angular-mocks.js',
                    '<%= pls.app %>/../common/lib/js/*underscore.js',
                    '<%= pls.app %>/../common/app/**/*.js',
                    '<%= pls.app %>/app/*loadjs.js',
                    '<%= pls.app %>/app/AppCommon/utilities/*.js',
                    '<%= pls.app %>/app/AppCommon/widgets/**/*.js',
                    '<%= pls.app %>/app/AppCommon/services/*.js',
                    '<%= pls.app %>/app/AppCommon/test/testData/*.js',
                    '<%= pls.app %>/app/AppCommon/test/unit/**/*.js',
                    '<%= pls.app %>/app/models/**/*.js',
                    '<%= pls.app %>/test/unit/**/*.js'
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
                    '/../common/app/**/!(angular|vendor|test)/!(*Spec).js': 'coverage'
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

        // Watches for changes in the given directories. Scripts watches for javaScript
        // changes, and reruns linting / unit tests. Css watches for changes in sass files
        // and recompiles production.css
        watch: {
            scripts: {
                files: [
                    '<%= pls.app %>/../common/app/**/*.js',
                    '<%= pls.app %>/app/**/*.js',
                    '<%= pls.app %>/app/app.js',
                    '<%= pls.app %>/test/**/*.js',
                    '!<%= pls.app %>/../common/lib/js/**/*.js'],
                tasks: ['jshint:dist', 'karma:watch:run']
            },
            css:     {
                files: [
                    '<%= pls.app %>/app/**/*.scss',
                    '<%= pls.app %>/assets/styles/**/*.scss'
                ],
                tasks: ['sass:dev']
            }
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
        },

        concurrent: {
            wget:    ['wget:angular', 'wget:crypto', 'wget:js', 'wget:css', 'wget:fonts', 'wget:kendojs', 'wget:kendocss', 'wget:kendofonts', 'wget:kendoimages'],
            test:    ['jshint:dist', 'karma:unit'],
            mac:     ['e2eChrome', 'e2eFirefox', 'e2eSafari'],
            windows: ['e2eChrome'],
            watch:   ['karma:watch', 'watch:scripts', 'watch:css']
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

        // predist and dist are used to copy index.html to a temporary folder for the processing
        // instrumented is to replace js by instrumented version, so that we can cc protractor test
        copy: {
            dist: {
                src:  'index.html',
                dest: '<%= pls.dist %>/index.html'
            },
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
            html:    'index.html',
            options: {
                dest: '<%= pls.dist %>',
                flow: {
                    html: {
                        steps: {
                            js:  ['concat'],
                            css: ['concat']
                        },
                        post:  {}
                    }
                }
            }
        },

        ngAnnotate: {
            dist: {
                files: {
                    '<%= pls.dist %>/app.js': ['<%= pls.dist %>/app.js']
                }
            }
        },

        // concat and compress js files
        uglify: {
            dist: {
                options: {
                    mangle: false
                },
                files: {
                    '<%= pls.app %>/app/production_<%= versionString %>.min.js': [
                        '<%= pls.app %>/app/AppCommon/vendor/date.format.js',
                        '<%= pls.app %>/app/AppCommon/vendor/ui-bootstrap-jpls-0.13.0.js',
                        '<%= pls.app %>/app/AppCommon/!(vendor|test)/**/*.js',
                        '<%= pls.app %>/app/!(AppCommon)/**/*.js',
                        '<%= pls.app %>/app/app.js'
                    ]
                }
            },
            production: {
                options: {
                    mangle: false
                },
                files: {
                    '<%= pls.dist %>/app.js': [
                        '<%= pls.dist %>/app.js'//,'<%= pls.dist %>/templates.js'
                    ]
                }
            }
        },

        // replace long list of local js files to compressed js and CDN links
        processhtml: {
            // redirect vendor javascript/css to minimized version on CDN
            options: {
                data: {
                    versionString: versionStringConfig
                }
            },

            dist: {
                files: {
                    '<%= pls.dist %>/index.html': ['<%= pls.dist %>/index.html']
                }
            }
        },

        html2js: {
            options: {
                base: ''
            },
            main: {
                src: ['app/**/*.html'],
                dest: '<%= pls.dist %>/templates.js'
            },
        }
    });

    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-html2js');
    grunt.loadNpmTasks('grunt-processhtml');
    grunt.loadNpmTasks('grunt-ng-annotate');
    grunt.loadNpmTasks('grunt-usemin');
    grunt.loadNpmTasks('grunt-rename');
    grunt.loadNpmTasks('grunt-replace');
    grunt.loadNpmTasks('grunt-karma');
    grunt.loadNpmTasks('grunt-http');
    grunt.loadNpmTasks('grunt-protractor-runner');
    grunt.loadNpmTasks('grunt-protractor-coverage');
    grunt.loadNpmTasks('grunt-istanbul');

    grunt.registerTask('build', [
        'clean:build',
        'html2js',
        'sass:dev',
        'copy:dist',
        'useminPrepare',
        'concat:generated',
        'ngAnnotate',
        'uglify:production',
        'usemin',
        'processhtml:dist'
    ]);

    grunt.registerTask('init', [
        'clean:lib',
        'concurrent:wget'
    ]);

    grunt.registerTask('dist', [
        'clean:dist',
        //'concurrent:test',
        'uglify:dist',
        'sass:dist',
        'processhtml:dist'
    ]);

    var defaultText = 'The default grunt build task. Runs a full build for everything including: file linting and minification (including css), running unit tests, and versioning for production. This website ready for distribution will be placed in the <SVN Directory>\<Product>\Projects\dist directory. This can be called just with the grunt command. The production files will then be named production_.js and production_.css.';
    grunt.registerTask('default', defaultText, [
        'dist',
        'clean:post',
        'clean:jsAndCss'
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
        //'karma:watch',
        //'watch:scripts',
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