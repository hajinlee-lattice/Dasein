'use strict';

module.exports = function (grunt) {

    var sourceDir = 'src/main/webapp/lp2';
    // Configurable paths for the application
    var appConfig = {
        app:  sourceDir,
        dist: 'dist',
        version: {
            jquery: '2.1.3',
            angular: '1.4.2',
            "angular-ui-bootstrap": '0.12.1',
            underscore: '1.8.2',
            qtip2: '2.2.1',
            webfont: '1.5.16',
            alasql: '0.2.0',
            d3: '3.5.6',
            crypto: '3.1.2',
            jStorage: '0.4.12',
            FileSaver: '2014-11-29',

            bootstrap: '3.3.4',
            "font-awesome": '4.3.0',

            kendo: '2015.2.805'
        }
    };

    // version of our software. This should really be in the package.json
    // but it gets passed in through
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();

    // Define the configuration for all the tasks
    grunt.initConfig({

        // Project settings
        pls:           appConfig,
        versionString: versionStringConfig,

        // download external libraries
        wget: {
            // download un-minimized version of vendor javascript libraries from CDN
            js: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'jquery/<%= pls.version.jquery %>/jquery.js',
                    'angular-ui-bootstrap/<%= pls.version["angular-ui-bootstrap"] %>/ui-bootstrap.js',
                    'angular-ui-bootstrap/<%= pls.version["angular-ui-bootstrap"] %>/ui-bootstrap-tpls.js',
                    'qtip2/<%= pls.version.qtip2 %>/jquery.qtip.js',
                    'underscore.js/<%= pls.version.underscore %>/underscore.js',
                    'webfont/<%= pls.version.webfont %>/webfontloader.js',
                    'twitter-bootstrap/<%= pls.version.bootstrap %>/js/bootstrap.js',
                    'd3/<%= pls.version.d3 %>/d3.js',
                    'jStorage/<%= pls.version.jStorage %>/jstorage.js',
                    'alasql/<%= pls.version.alasql %>/alasql.min.js',
                    'FileSaver.js/<%= pls.version.FileSaver %>/FileSaver.js'
                ],
                dest: '<%= pls.app %>/lib/js'
            },

            angular: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/angular.js/<%= pls.version.angular %>/'
                },
                src: [
                    'angular.js',
                    'angular-resource.js',
                    'angular-route.js',
                    'angular-sanitize.js',
                    'angular-mocks.js'
                ],
                dest: '<%= pls.app %>/lib/js/angular'
            },

            crypto: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/crypto-js/<%= pls.version.crypto %>/components/'
                },
                src: [
                    'core.js',
                    'sha256.js'
                ],
                dest: '<%= pls.app %>/lib/js/crypto'
            },

            css: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'bootswatch/<%= pls.version.bootstrap %>/simplex/bootstrap.css',
                    'qtip2/<%= pls.version.qtip2 %>/jquery.qtip.css',
                    'font-awesome/<%= pls.version["font-awesome"] %>/css/font-awesome.css',
                    'font-awesome/<%= pls.version["font-awesome"] %>/css/font-awesome.css.map'
                ],
                dest: '<%= pls.app %>/lib/css'
            },

            fonts: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'font-awesome/<%= pls.version["font-awesome"] %>/fonts/FontAwesome.otf',
                    'font-awesome/<%= pls.version["font-awesome"] %>/fonts/fontawesome-webfont.eot',
                    'font-awesome/<%= pls.version["font-awesome"] %>/fonts/fontawesome-webfont.svg',
                    'font-awesome/<%= pls.version["font-awesome"] %>/fonts/fontawesome-webfont.ttf',
                    'font-awesome/<%= pls.version["font-awesome"] %>/fonts/fontawesome-webfont.woff',
                    'font-awesome/<%= pls.version["font-awesome"] %>/fonts/fontawesome-webfont.woff2'
                ],
                dest: '<%= pls.app %>/lib/fonts'
            },

            kendojs: {
                options: {
                    baseUrl: 'http://kendo.cdn.telerik.com/<%= pls.version.kendo %>/js/'
                },
                src: ['kendo.all.min.js', 'kendo.all.min.js'],
                dest: '<%= pls.app %>/lib/js'
            },

            kendocss: {
                options: {
                    baseUrl: 'http://kendo.cdn.telerik.com/<%= pls.version.kendo %>/styles/'
                },
                src: [
                    'kendo.common-bootstrap.min.css',
                    'kendo.bootstrap.min.css',
                    'kendo.dataviz.min.css',
                    'kendo.dataviz.bootstrap.min.css',
                    'kendo.mobile.all.min.css'
                ],
                dest: '<%= pls.app %>/lib/css'
            },

            kendofonts: {
                options: {
                    baseUrl: 'http://kendo.cdn.telerik.com/<%= pls.version.kendo %>/styles/images/'
                },
                src: ['kendoui.woff', 'kendoui.woff'],
                dest: '<%= pls.app %>/lib/css/images'
            },

            kendoimages: {
                options: {
                    baseUrl: 'http://kendo.cdn.telerik.com/<%= pls.version.kendo %>/styles/'
                },
                src: ['Bootstrap/sprite.png', 'Bootstrap/loading-image.gif'],
                dest: '<%= pls.app %>/lib/css/Bootstrap'
            }
        },

        // Removes unessasary folders and files that are created during the build process
        // Force = true to allow for deleting contents outside of the grunt directory structure
        clean: {
            lib: {
                files:   [{
                    dot: true,
                    src: ['<%= pls.app %>/lib']
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
                    '<%= pls.app %>/assets/styles/production_<%= versionString %>.min.css': '<%= pls.app %>/assets/styles/main.scss'
                }
            },
            dev:     {
                files: {
                    '<%= pls.app %>/assets/styles/production.css': '<%= pls.app %>/assets/styles/main.scss'
                }
            }
        },

        // replace long list of local js files to compressed js and CDN links
        processhtml: {
            // redirect vendor javascript/css to minimized version on CDN
            options: {
                data: {
                    version: appConfig.version,
                    versionString: versionStringConfig
                }
            },

            dist: {
                files: {
                    '<%= pls.app %>/index.html': ['<%= pls.app %>/index.html']
                }
            }
        },

        // predist and dist are used to copy index.html to a temporary folder for the processing
        // instrumented is to replace js by instrumented version, so that we can cc protractor test
        copy: {
            dist: {
                src:  '.tmp/index.html',
                dest: '<%= pls.app %>/index.html'
            },

            instrumented: {
                files: [{
                    expand: true,
                    cwd: 'target/protractor_coverage/instrumented/src/main/webapp/',
                    src: ['**/*'],
                    dest: '<%= pls.app %>'
                }]
            }
        },

        instrument: {
            files: '<%= pls.app %>/app/**/**[!vendor]/*[!Spec].js',
            options: {
                lazy: true,
                basePath: "target/protractor_coverage/instrumented"
            }
        },

        concurrent: {
            wget:    ['wget:angular', 'wget:crypto', 'wget:js', 'wget:css', 'wget:fonts', 'wget:kendojs', 'wget:kendocss', 'wget:kendofonts', 'wget:kendoimages']
        }

    });

    grunt.loadNpmTasks('grunt-wget');
    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-processhtml');
    grunt.loadNpmTasks('grunt-ng-annotate');
    grunt.loadNpmTasks('grunt-istanbul');


    grunt.registerTask('init', [
        'clean:lib',
        'concurrent:wget'
    ]);

    grunt.registerTask('dist', [
        'clean:dist',
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

    var devText = 'Compiles sass into css, and checks javascript files for errors. This needs to be run if you don\'t have sentry running, and you don\'t have a production.css file in the styles directory';
    grunt.registerTask('dev', devText, [
        'clean:post',
        'jshint:dist',
        'sass:dev'
    ]);

    var instrumentJsText = 'Instrument javascript code for code coverage';
    grunt.registerTask('instrumentJs', instrumentJsText, [
        'instrument',
        'copy:instrumented'
    ]);

};