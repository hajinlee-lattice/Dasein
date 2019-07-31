import { createStore, combineReducers, applyMiddleware } from "redux";
import thunk from "redux-thunk";
const createReducer = asyncReducers => {
	return combineReducers({
		...asyncReducers
	});
};

const middleware = [thunk];

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose;

export default function configureStore(initialState = {}) {
	let store = createStore(
		createReducer(),
		initialState,
		composeEnhancers(applyMiddleware(...middleware))
	);
	store.asyncReducers = {};
	return store;
}

if (!window.store) {
	window.store = configureStore();
}

export const store = window.store;

export function injectAsyncReducer(store, name, asyncReducer) {
	store.asyncReducers[name] = asyncReducer;
	store.replaceReducer(createReducer(store.asyncReducers));
}

export function mount(path) {
	return state => {
		if (path) {
			return { store: { ...state[path] } };
		} else {
			return { store: { ...state } };
		}
	};
}
