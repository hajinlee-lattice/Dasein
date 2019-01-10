const webpack = require('webpack');
const merge = require('webpack-merge');
const path = require('path');
const common = require('./webpack.common.js');

const MiniCssExtractPlugin = require('mini-css-extract-plugin');
//const HardSourceWebpackPlugin = require("hard-source-webpack-plugin");

module.exports = merge(common, {
    mode: 'development',
    devtool: 'module-source-map',
    output: {
        devtoolModuleFilenameTemplate(info) {
            return `file:///${info.absoluteResourcePath.replace(/\\/g, '/')}`;
        }
    },
    stats: {
        colors: true
    },
    plugins: [
        //new HardSourceWebpackPlugin(),
        new MiniCssExtractPlugin({
            filename: '[name].css'
        })
    ],
    module: {
        rules: [
            {
                test: /\.s?css/,
                exclude: '/node_modules/',
                use: [
                    MiniCssExtractPlugin.loader,
                    {
                        loader: 'css-loader',
                        options: {
                            sourceMap: true,
                            importLoaders: 1
                        }
                    },
                    {
                        loader: 'sass-loader',
                        options: {
                            sourceMap: true,
                            importLoaders: 1
                        }
                    }
                ]
            }
        ]
    },
    watchOptions: {
        aggregateTimeout: 300,
        ignored: '/node_modules/'
    }
});
