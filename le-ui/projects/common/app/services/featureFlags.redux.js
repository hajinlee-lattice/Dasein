import { store } from "../store/index";
import { isEmpty } from "../utilities/ObjectUtilities.js";
var CONST = {
  FETCH_FEATURE_FLAGS: "FETCH_FEATURE_FLAGS",
  SET_FEATURE_FLAGS: "SET_FEATURE_FLAGS"
};
const initialState = {
  featureFlags: {}
};

export const actions = {
  setFeatureFlags: featureflags => {
    if (!isEmpty(featureflags)) {
      return store.dispatch({
        type: CONST.SET_FEATURE_FLAGS,
        payload: featureflags
      });
    }
  }
};

export const reducer = (state = initialState, action) => {
  switch (action.type) {
    case CONST.FETCH_FEATURE_FLAGS:
      return {
        ...state,
        featureFlags: action.payload
      };
    case CONST.SET_FEATURE_FLAGS:
      return {
        ...state,
        featureFlags: action.payload
      };
    default:
      return state;
  }
};
