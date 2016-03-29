module.exports = function(grunt) {
	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		dir: {
			assets: './assets',
			app: './app',
			bower: './lib/bower',
			dist: './assets'
		},
		sass: {
			dist: {
				options: {
					style: 'compressed'
				},
				files: {
					'<%= dir.dist %>/lattice.css' : './assets/sass/lattice.scss'
				}
			}
		},
		watch: {
			js: {
				files: '<%= dir.app %>/**/*.js',
				tasks: [
					'concat:production',
					'uglify:production',
					'concat:dist',
				]
			},
			css: {
				files: '<%= dir.assets %>/sass/*.scss',
				tasks: ['sass:dist']
			}
		},
        ngAnnotate: {
            production: {
                files: {
                    '<%= dir.assets %>/lattice.min.js': [
                    	'<%= dir.assets %>/lattice.min.js'
                    ]
                }
            }
        },
        uglify: {
            vendor: {
                options: {
                    mangle: false
                },
                files: {
                    '<%= dir.assets %>/vendor.min.js': [
                        '<%= dir.assets %>/vendor.min.js'
                    ]
                }
            },
            production: {
                options: {
                    mangle: false
                },
                files: {
                    '<%= dir.assets %>/lattice.min.js': [
                        '<%= dir.assets %>/lattice.min.js'
                    ]
                }
            }
        },
        concat: {
			vendor: {
				src: [
					'<%= dir.bower %>/webfontloader.js',
					'<%= dir.bower %>/min/jquery*.js',
					'<%= dir.bower %>/min/angular.min.js',
					'<%= dir.bower %>/min/*.js',
					'<%= dir.bower %>/*.js'
				],
				dest: '<%= dir.assets %>/vendor.min.js'
			},
			production: {
				src: [
					'<%= dir.app %>/**/*.js'
				],
				dest: '<%= dir.assets %>/lattice.min.js'
			}
		},
        concurrent: {
            sentry: ['watch:js','watch:css']
        }
	});

	grunt.loadNpmTasks('grunt-contrib-sass');
	grunt.loadNpmTasks('grunt-contrib-watch');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-concurrent');
    grunt.loadNpmTasks('grunt-ng-annotate');

	grunt.registerTask('build',[
		'concat:vendor',
		//'uglify:vendor',
		'concat:production',
        'ngAnnotate:production',
		'uglify:production',
		'sass:dist'
	]);

	grunt.registerTask('sentry',[
		'concurrent:sentry'
	]);
};