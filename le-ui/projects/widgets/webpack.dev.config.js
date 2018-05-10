const path = require('path');
const merge = require('webpack-merge');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const baseConfig = require('./webpack.base.config.js');

module.exports = merge(baseConfig, {
    devtool: 'eval-source-map',
    entry: './src/dev-main.js',

    devServer: {
        inline: true,
        contentBase: 'src',
        port: '8889',
        historyApiFallback: true
    },

    module: {
        rules: []
    },
    plugins: [
        new HtmlWebpackPlugin({
            template: './src/index.html'
        })
    ]
});