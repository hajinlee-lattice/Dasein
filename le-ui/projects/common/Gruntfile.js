module.exports = function(grunt) {
	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		dir: {
			assets: './assets',
			app: './app',
			bower: './lib/bower'
		},
		sass: {
			dist: {
				options: {
					style: 'compressed'
				},
				files: {
					'./assets/css/lattice.css' : './assets/sass/lattice.scss'
				}
			}
		},
		watch: {
			css: {
				files: './assets/sass/*.scss',
				tasks: ['sass:dist']
			}
		},
        uglify: {
            dist: {
                options: {
                    mangle: false
                },
                files: {
                    '<%= dir.assets %>/js/min/vendor.min.js': [
                        '<%= dir.bower %>/*.min.js'
                    ]
                }
            }
        },
        concat: {
			vendor: {
				src: [
					'<%= dir.bower %>/min/jquery*.js',
					'<%= dir.bower %>/min/angular.min.js',
					'<%= dir.bower %>/min/*.js',
					'<%= dir.bower %>/*.js'
				],
				dest: '<%= dir.assets %>/js/min/vendor.min.js'
			},
			dist: {
				src: [
					'<%= dir.app %>/directives/*.js',
					'<%= dir.app %>/utilities/*.js',
					'<%= dir.app %>/services/*.js',
					'<%= dir.app %>/modals/*.js'
				],
				dest: '<%= dir.assets %>/js/min/production.min.js'
			}
		}
	});
	grunt.loadNpmTasks('grunt-contrib-sass');
	grunt.loadNpmTasks('grunt-contrib-watch');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-contrib-concat');

	grunt.registerTask('default',['concat:dist','concat:vendor', 'sass:dist']);
	grunt.registerTask('sentry',['watch:css']);
};