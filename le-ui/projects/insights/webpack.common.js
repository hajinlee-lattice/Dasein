const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CleanWebpackPlugin = require("clean-webpack-plugin");
const webpack = require("webpack");

module.exports = {
    entry: {
        angular: "../common/angular-vendor.index.js",
        vendor: "../common/vendor.index.js",
        insights: "./index.js"
    },

    plugins: [
        new CleanWebpackPlugin(["dist"]),
        new HtmlWebpackPlugin({
            filename: "indexwp.html",
            template: __dirname + "/indexwp.html"
        })
    ],

    output: {
        filename: "[name].bundle.js",
        path: path.resolve(__dirname, "dist")
    },

    module: {
        rules: [
            {
                test: /\.(png|svg|jpg|gif)$/,
                exclude: /node_modules/,
                use: ["file-loader"]
            },
            {
                test: /\.(woff|woff2|eot|ttf|otf)$/,
                exclude: /node_modules/,
                use: ["file-loader"]
            },
            { test: /\.html$/, exclude: /node_modules/, use: ["raw-loader"] },
            {
                test: /\.js$/,
                exclude: /node_modules/,
                use: { loader: 'babel-loader', options:{
                    presets: [
                        "@babel/preset-env",
                    ],
                    plugins: [
                        "angularjs-annotate",
                    ]
                    } 
                }
            }
        ]
    }
};
