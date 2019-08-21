import * as reduxStore from "./redux-store";
import ngRedux from "ng-redux";
import lodash from "lodash";

export const store = reduxStore.store;
export const injectAsyncReducer = reduxStore.injectAsyncReducer;
export const mount = reduxStore.mount;
const configureStore = reduxStore.configureStore;
export default configureStore;

angular
	.module("mainApp.core.redux", [ngRedux])
	.service("ReduxService", function($ngRedux) {
		this.connect = function(name, actions, reducer, context) {
			context = context || {};

			if (!context.data) {
				context.data = {};
			}

			let unsubscribe = $ngRedux.connect(reduxStore.mount(name), {
				...actions
			})((context.data.redux = {}));

			context.data.redux.unsubscribe = unsubscribe;

			reduxStore.injectAsyncReducer(store, name, reducer);

			return context.data.redux;
		};
	})
	.config($ngReduxProvider => {
		$ngReduxProvider.provideStore(store);
		// window.__REDUX_DEVTOOLS_EXTENSION__()
	});
