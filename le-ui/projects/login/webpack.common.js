const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CleanWebpackPlugin = require("clean-webpack-plugin");
const webpack = require("webpack");

module.exports = {
  entry: {
    angular: "../common/angular-vendor.index.js",
    vendor: "../common/vendor.index.js",
    login: "./index.js"
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
              "@babel/preset-react"
          ],
          plugins: [
              "@babel/plugin-transform-spread",
              "angularjs-annotate",
              "@babel/plugin-syntax-dynamic-import",
              "@babel/plugin-syntax-import-meta",
              "@babel/plugin-proposal-class-properties",
              "@babel/plugin-proposal-json-strings",
              [
                  "@babel/plugin-proposal-decorators",
                  {
                      "legacy": true
                  }
              ],
              "@babel/plugin-proposal-function-sent",
              "@babel/plugin-proposal-export-namespace-from",
              "@babel/plugin-proposal-numeric-separator",
              "@babel/plugin-proposal-throw-expressions"
          ]
          } 
      }
      }
    ]
  }
};
