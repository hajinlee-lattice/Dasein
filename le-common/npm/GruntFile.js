module.exports = function(grunt) {
    // Configurable paths for the application
    var appConfig = {
        dir:  'src',
        dist: 'dist',
        vendor: {
            jquery: '2.1.3',
            angular: '1.3.15',
            "angular-local-storage": '0.1.5',
            underscore: '1.8.3'
        }
    };

    grunt.initConfig({
        app: appConfig,

        pkg: grunt.file.readJSON("package.json"),

        wget: {
            // download un-minimized version of vendor javascript libraries from CDN
            default: {
                options: {
                    baseUrl: 'http://cdnjs.cloudflare.com/ajax/libs/'
                },
                src: [
                    'jquery/<%= app.vendor.jquery %>/jquery.js',
                    'angular.js/<%= app.vendor.angular %>/angular.js',
                    'angular.js/<%= app.vendor.angular %>/angular-sanitize.js',
                    'angular-local-storage/<%= app.vendor["angular-local-storage"] %>/angular-local-storage.js',
                    'underscore.js/<%= app.vendor.underscore %>/underscore.js'
                ],
                dest: 'lib/js'
            }
        },

        concat: {
            default: {
                src: [
                    'src/util/UnderscoreUtility.js',
                    'src/util/BrowserStorageUtility.js',
                    'src/util/SessionUtility.js'
                ],
                dest: 'release/<%= pkg.version %>/le-common.js'
            }
        },

        uglify: {
            options: {
                mangle: false
            },
            // the order of source files matters!
            default: {
                files: {
                    'release/<%= pkg.version %>/le-common.min.js': ['release/<%= pkg.version %>/le-common.js']
                }
            }
        },

        jshint: {
            options: {
                reporter: require('jshint-stylish')
            },

            default: [
                'Gruntfile.js',
                'src/**/*.js',
                '!src/**/*Spec.js',
                '!src/**/*spec.js'
            ]
        }
    });

    grunt.loadNpmTasks('grunt-wget');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-jshint');

    // main task to run before deploy the dist war
    grunt.registerTask('dist', ['unit', 'concat', 'uglify']);

    // download vendor javascript
    grunt.registerTask('init', ['clean:lib', 'wget']);

    grunt.registerTask('unit', ['jshint']);

};