const webpack = require('webpack');
const fs = require('fs-extra');
const merge = require('webpack-merge');
const ExtractTextPlugin = require('extract-text-webpack-plugin');
const UglifyJsPlugin = require('uglifyjs-webpack-plugin');
const path = require('path');
const WebpackShellPlugin = require('webpack-shell-plugin');
const WebpackOnBuildPlugin = require('on-build-webpack');
const UglifyJSPlugin = require('uglifyjs-webpack-plugin');
const baseConfig = require('./webpack.base.config.js');

module.exports = merge(baseConfig, {
    entry: './src/components-main.js',
    output: {
        path: path.join(__dirname, '/dist'),
        filename: 'le-widgets.js',
    },

    module: {
        rules: [],
    },

    plugins: [
        // Extract imported CSS into own file
        new WebpackShellPlugin({
            onBuildStart: ['echo "LE-Widgets build Starts"'],
            onBuildEnd: ['echo "LE-Widgets build Ended"']
        }),
        
        new WebpackOnBuildPlugin(function (stats) {
                try {
                    fs.remove('./dist', err =>{
                        if (err) return console.error(err);

                        console.log('success!'); // I just deleted my entire HOME directory.
                    });
                    // fs.emptyDir('./tmp', err => {

                    // });
                    fs.copySync('./dist/le-widgets.js', '../common/lib/bower/le-widgets.js');

                } catch (err) {
                    console.error(err)
                }
        }),
        // Minify JS
        new UglifyJsPlugin({
            sourceMap: true
        }),
        // Minify CSS
        new webpack.LoaderOptionsPlugin({
            minimize: true,
        }),
    ],
});