'use strict';

module.exports = function (grunt) {
    // version of our software. This should really be in the package.json
    // but it gets passed in through 
    var versionStringConfig = grunt.option('versionString') || new Date().getTime();

    // Define the configuration for all the tasks
    grunt.initConfig({
        // Project settings
        pls: {
            assets: 'assets',
            app: 'app',
            lib: 'lib',
            dist: 'assets'
        },
        versionString: versionStringConfig,

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
                        '<%= pls.app %>/production_*.js',
                        '<%= pls.assets %>/styles/production_*.css'
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
                    '<%= pls.assets %>/styles/production_<%= versionString %>.min.css': '<%= pls.app %>/app.scss'
                }
            },
            dev:     {
                files: {
                    '<%= pls.assets %>/styles/production.css': '<%= pls.app %>/app.scss'
                }
            }
        },

        // runs error checking on all of our (not already minified) javascript code
        jshint: {
            dist: {
                src:     [
                    '<%= pls.app %>/**/*.js',
                    '!<%= pls.app %>/AppCommon/widgets/talkingPointWidget/TalkingPointParser.js',
                    '!<%= pls.app %>/AppCommon/vendor/**/*.js',
                    '!<%= pls.app %>/AppCommon/test/**/*.js'
                ],
                options: {
                    eqnull: true,
                    sub: true
                }
            }
        },

        // Watches for changes in the given directories. Scripts watches for javaScript
        // changes, and reruns linting / unit tests. Css watches for changes in sass files
        // and recompiles production.css
        watch: {
            scripts: {
                files: ['<%= pls.app %>/**/*.js',
                    '<%= pls.app %>/app.js',
                    '/test/**/*.js',
                    '!<%= pls.app %>/app/AppCommon/vendor/**/*.js'],
                tasks: ['jshint:dist', 'karma:watch:run']
            },
            css:     {
                files: ['<%= pls.assets %>/styles/**/*.scss'],
                tasks: ['sass:dev']
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

        // alters angular js code to use DI conventions that dont break on minification
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
                        '<%= pls.dist %>/app.js',
                        '<%= pls.dist %>/templates.js'
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

        // converts html to an angular-friendly module for pre-populating ng $templateCache
        html2js: {
            options: {
                base: ''
            },
            main: {
                src: ['<%= pls.app %>/**/*.html'],
                dest: '<%= pls.dist %>/templates.js'
            },
        }
    });

    grunt.loadNpmTasks('grunt-contrib-clean');
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-copy');
    grunt.loadNpmTasks('grunt-contrib-jshint');
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-html2js');
    grunt.loadNpmTasks('grunt-processhtml');
    grunt.loadNpmTasks('grunt-ng-annotate');
    grunt.loadNpmTasks('grunt-usemin');
    grunt.loadNpmTasks('grunt-rename');
    grunt.loadNpmTasks('grunt-replace');

    grunt.registerTask('build', [
        'clean:build',
        'html2js',
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
        'concurrent:test',
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

    var lintText = 'Checks all JavaScript code for possible errors. This should be run before a checkin if you aren\'t using grunt sentry';
    grunt.registerTask('lint', lintText, [
        'jshint:dist'
    ]);

    var sentryText = 'Watches for changes in any javascript file, and automatically re-runs linting and karma unit tests. If your computer can handle the strain, this should be running during active develpment';
    grunt.registerTask('sentry', sentryText, [
        'nodemon:dev',
        'watch:scripts',
        'watch:css'
    ]);
};