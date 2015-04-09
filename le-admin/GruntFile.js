module.exports = function(grunt) {
    var sourceDir = 'src/main/webapp';
    // Configurable paths for the application
    var appConfig = {
        dir:  sourceDir,
        dist: 'dist',
        distDir: 'dist/' + sourceDir,
        versionString: new Date().getTime(),
        vendor: {
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
                    'jquery/<%= app.vendor.jquery %>/jquery.js',
                    'angular.js/<%= app.vendor.angular %>/angular.js',
                    'angular.js/<%= app.vendor.angular %>/angular-sanitize.js',
                    'angular-local-storage/<%= app.vendor["angular-local-storage"] %>/angular-local-storage.js',
                    'angular-ui-router/<%= app.vendor["angular-ui-router"] %>/angular-ui-router.js',
                    'underscore.js/<%= app.vendor.underscore %>/underscore.js',
                    'webfont/<%= app.vendor.webfont %>/webfontloader.js',
                    'twitter-bootstrap/<%= app.vendor.bootstrap %>/js/bootstrap.js'
                ],
                dest: '<%= app.dir %>/app/vendor'
            },

            css: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'bootswatch/<%= app.vendor.bootstrap %>/simplex/bootstrap.css',
                    'font-awesome/<%= app.vendor["font-awesome"] %>/css/font-awesome.css',
                    'font-awesome/<%= app.vendor["font-awesome"] %>/css/font-awesome.css.map'
                ],
                dest: '<%= app.dir %>/assets/style/vendor'
            },

            fonts: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'font-awesome/<%= app.vendor["font-awesome"] %>/fonts/FontAwesome.otf',
                    'font-awesome/<%= app.vendor["font-awesome"] %>/fonts/fontawesome-webfont.eot',
                    'font-awesome/<%= app.vendor["font-awesome"] %>/fonts/fontawesome-webfont.svg',
                    'font-awesome/<%= app.vendor["font-awesome"] %>/fonts/fontawesome-webfont.ttf',
                    'font-awesome/<%= app.vendor["font-awesome"] %>/fonts/fontawesome-webfont.woff',
                    'font-awesome/<%= app.vendor["font-awesome"] %>/fonts/fontawesome-webfont.woff2'
                ],
                dest: '<%= app.dir %>/assets/style/fonts'
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
                        '<%= app.dir %>/app/core/directive/MainNavDirective.js',
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
            backup: {
                expand: true,
                src: ['<%= app.dir %>/*.html'],
                dest: '<%= app.dist %>'
            },
            restore: {
                expand: true,
                cwd: '<%= app.distDir %>',
                src: '*.html',
                dest: '<%= app.dir %>'
            }
        },

        processhtml: {
            // redirect vendor javascript/css to minimized version on CDN
            options: {
                data: {
                    version: appConfig.vendor,
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
    grunt.registerTask('init', ['clean:vendor', 'wget:js', 'wget:css', 'wget:fonts', 'less:dev']);

    grunt.registerTask('unit', ['jshint']);

    // restore dev setup after run a dist
    grunt.registerTask('restore', ['copy:restore', 'clean:restore']);

};