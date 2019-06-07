const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CleanWebpackPlugin = require("clean-webpack-plugin");
const webpack = require("webpack");

module.exports = {
	resolve: {
		alias: {
			common: path.resolve(__dirname, "../common"),
			app: path.resolve(__dirname, "../common/app"),
			components: path.resolve(__dirname, "../common/components"),
			widgets: path.resolve(__dirname, "../common/widgets"),
			atlas: path.resolve(__dirname, "app"),
			assets: path.resolve(__dirname, "assets")
		}
	},
  entry: {
    angular: "../common/angular-vendor.index.js",
    vendor: "../common/vendor.index.js",
    visualization: "../common/vendor-visualization.index.js",
    lpi: "./index.js"
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
        use: { loader: "babel-loader" }
      }
    ]
  }
};
