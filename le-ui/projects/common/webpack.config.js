const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const CleanWebpackPlugin = require("clean-webpack-plugin");
const webpack = require("webpack");
//const HardSourceWebpackPlugin = require("hard-source-webpack-plugin");

module.exports = {
	resolve: {
		alias: {
			common: path.resolve(__dirname, "."),
			app: path.resolve(__dirname, "./app"),
			assets: path.resolve(__dirname, "./assets"),
			components: path.resolve(__dirname, "./components"),
			widgets: path.resolve(__dirname, "./widgets")
		}
	},
	entry: {
		widgets: "./widgets/index.js"
	},
	devtool: "module-source-map",
	plugins: [
		//new HardSourceWebpackPlugin(),
		new CleanWebpackPlugin(["dist"]),
		new HtmlWebpackPlugin({
			filename: "index.html",
			template: __dirname + "/index.html"
		})
	],

	output: {
		devtoolModuleFilenameTemplate(info) {
			return `file:///${info.absoluteResourcePath.replace(/\\/g, "/")}`;
		},
		filename: "[name].bundle.js",
		path: path.resolve(__dirname, "dist")
	},
	module: {
		rules: [
			{
				test: /\.s?css/,
				use: [
					{ loader: "style-loader" },
					{
						loader: "css-loader",
						options: {
							sourceMap: false
						}
					},
					{
						loader: "sass-loader",
						options: {
							sourceMap: false
						}
					}
				]
			},
			{ test: /\.(png|svg|jpg|gif)$/, use: ["file-loader"] },
			{ test: /\.(woff|woff2|eot|ttf|otf)$/, use: ["file-loader"] },
			{ test: /\.html$/, use: ["raw-loader"] },
			{
				test: /\.js$/,
				exclude: /node_modules/,
				use: {
					loader: "babel-loader",
					options: {
						presets: ["@babel/preset-env", "@babel/preset-react"],
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
									legacy: true
								}
							],
							"@babel/plugin-proposal-function-sent",
							"@babel/plugin-proposal-export-namespace-from",
							"@babel/plugin-proposal-numeric-separator",
							"@babel/plugin-proposal-throw-expressions",
							"@babel/plugin-proposal-nullish-coalescing-operator",
							"@babel/plugin-proposal-optional-chaining"
						]
					}
				}
			}
		]
	}
};
