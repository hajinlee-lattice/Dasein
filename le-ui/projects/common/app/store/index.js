import { createStore, combineReducers, applyMiddleware } from "redux";
import thunk from "redux-thunk";
import ngRedux from "ng-redux";

const createReducer = asyncReducers => {
    return combineReducers({
        ...asyncReducers
    });
};

export default function configureStore(initialState = {}) {
    let store = createStore(
        createReducer(),
        initialState,
        applyMiddleware(thunk)
    );
    store.asyncReducers = {};
    return store;
}

export const store = configureStore();

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

angular
    .module("mainApp.core.redux", [ngRedux])
    .service("ReduxService", function($ngRedux) {
        this.connect = function(name, actions, reducer, context) {
            console.log(name, context);
            if (!context.data) {
                context.data = {};
            }

            let unsubscribe = $ngRedux.connect(
                mount(name),
                { ...actions }
            )((context.data.redux = {}));

            context.data.redux.unsubscribe = unsubscribe;

            injectAsyncReducer(store, name, reducer);

            return {
                name,
                actions,
                unsubscribe,
                context
            };
        };
    })
    .config($ngReduxProvider => {
        $ngReduxProvider.provideStore(store);
        // window.__REDUX_DEVTOOLS_EXTENSION__()
    });
