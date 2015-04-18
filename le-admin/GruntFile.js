module.exports = function(grunt) {
    var sourceDir = 'src/main/webapp';
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
            underscore: '1.8.3',
            webfont: '1.5.16',

            bootstrap: '3.3.4',
            "font-awesome": '4.3.0',

            kendo: '2015.1.408'
        },
        env: {
            dev: {
                url: 'http://localhost:8080'
            }
        }
    };

    grunt.initConfig({
        app: appConfig,

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
                    'angular-local-storage/<%= app.version["angular-local-storage"] %>/angular-local-storage.js',
                    'angular-ui-router/<%= app.version["angular-ui-router"] %>/angular-ui-router.js',
                    'angular-ui-bootstrap/<%= app.version["angular-ui-bootstrap"] %>/ui-bootstrap.js',
                    'angular-ui-bootstrap/<%= app.version["angular-ui-bootstrap"] %>/ui-bootstrap-tpls.js',
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
                        '<%= app.distDir %>/app/core/util/SessionUtility.js',
                        '<%= app.distDir %>/app/core/directive/MainNavDirective.js',
                        '<%= app.distDir %>/app/login/service/LoginService.js',
                        '<%= app.distDir %>/app/login/controller/LoginCtrl.js',
                        '<%= app.distDir %>/app/tenants/service/TenantService.js',
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
                '<%= app.dist %>',
                '<%= app.www %>'
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
                reporter: require('jshint-stylish')
            },

            default: [
                'Gruntfile.js',
                '<%= app.dir %>/app/app.js',
                '<%= app.dir %>/app/**/*.js',
                '!<%= app.dir %>/app/**/*Spec.js'
            ]
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

    // main task to run before deploy the dist war
    grunt.registerTask('dist', [
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
        'wget:js', 'wget:css', 'wget:fonts',
        'wget:kendojs', 'wget:kendocss', 'wget:kendofonts', 'wget:kendoimages',
        'less:dev']);

    grunt.registerTask('www', ['copy:www', 'clean:www']);

    grunt.registerTask('unit', ['jshint']);

    // restore dev setup after run a default task
    grunt.registerTask('restore', ['copy:restorewww','copy:restore', 'clean:restore', 'less:dev']);

};