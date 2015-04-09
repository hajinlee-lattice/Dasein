module.exports = function(grunt) {
    var sourceDir = 'src/main/webapp';
    // Configurable paths for the application
    var appConfig = {
        dir:  sourceDir,
        dist: 'dist',
        distDir: 'dist/' + sourceDir,
        leCommon: '../le-common/npm/release',
        versionString: new Date().getTime(),
        version: {
            "le-common": '0.0.1',

            jquery: '2.1.3',
            angular: '1.3.15',
            "angular-local-storage": '0.1.5',
            "angular-ui-router": '0.2.13',
            underscore: '1.8.3',
            webfont: '1.5.16',

            bootstrap: '3.3.4',
            "font-awesome": '4.3.0'
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
            }
        },

        uglify: {
            options: {
                mangle: false
            },
            // the order of source files matters!
            default: {
                files: {
                    '<%= app.dir %>/assets/js/app_<%= app.versionString %>.min.js': [
                        '<%= app.dir %>/app/lib/le-common.js',
                        '<%= app.dir %>/app/core/directive/MainNavDirective.js',
                        '<%= app.dir %>/app/tenants/controller/TenantsCtrl.js',
                        '<%= app.dir %>/app/tenants/controller/TenantInfoCtrl.js',
                        '<%= app.dir %>/app/app.js'
                    ]
                }
            },
            // it is temporary to compile le-common.js in this project. it will be moved to le-common later.
            common: {
                files: {
                    '<%= app.dir %>/assets/js/app_<%= app.versionString %>.min.js': [
                        '<%= app.dir %>/app/LECommon/util/MainNavDirective.js',
                        '<%= app.dir %>/app/tenants/controller/TenantsCtrl.js',
                        '<%= app.dir %>/app/tenants/controller/TenantInfoCtrl.js',
                        '<%= app.dir %>/app/app.js'
                    ]
                }
            }
        },

        less: {
            default: {
                files: {
                    "<%= app.dir %>/assets/css/main_compiled.css": "<%= app.dir %>/assets/less/main.less"
                }
            }
        },

        cssmin: {
            default: {
                files: {
                    '<%= app.dir %>/assets/css/main_<%= app.versionString %>.min.css': ['<%= app.dir %>/assets/css/main_compiled.css']
                }
            }
        },

        clean: {
            vendor: ['<%= app.dir %>/lib'],
            restore: [
                '<%= app.dir %>/assets/js',
                '<%= app.dir %>/assets/css/main_*.min.css',
                '<%= app.dist %>'
            ]
        },

        copy: {
            lecommon: {
                expand: true,
                cwd: '<%= app.leCommon %>/<%= app.version["le-common"] %>',
                src: ['le-common.js'],
                dest: '<%= app.dir %>/lib/js'
            },
            backup: {
                expand: true,
                src: ['<%= app.dir %>/*.html'],
                dest: '<%= app.dist %>'
            },
            restore: {
                expand: true,
                cwd: '<%= app.dist %>/<%= app.dir %>',
                src: '*.html',
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
                    '<%= app.dir %>/index.html': ['<%= app.distDir %>/index.html']
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
    grunt.registerTask('default', [
        'uglify',
        'less',
        'cssmin',
        'copy:backup',
        'processhtml:dist'
    ]);

    // download vendor javascript and css
    grunt.registerTask('init', ['clean:vendor', 'copy:lecommon', 'wget:js', 'wget:css', 'wget:fonts', 'less']);

    grunt.registerTask('unit', ['jshint']);

    // restore dev setup after run a dist
    grunt.registerTask('restore', ['copy:restore', 'clean:restore']);

};