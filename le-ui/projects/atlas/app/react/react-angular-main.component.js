import React, { Component, react2angular, UIRouter, UIView } from "common/react-vendor";
import ReactRouter from './router';
import './react-main.component.scss';
import LeModal from 'common/widgets/modal/le-modal';
import { store, injectAsyncReducer } from 'store';
import { REDUX_STATE_MODAL, REDUX_STATE_BANNER } from "./redux.states";
import LeBanner from "../../../common/widgets/banner/le-banner";

export default class ReactAngularMainComponent extends Component {
    constructor(props) {
        super(props);
        console.log(this.props);
    }


    componentDidMount() {
        // console.log('ROUTER', ReactRouter);
        // console.log('THE STORE ==> ', store);
        let router = ReactRouter.getRouter();
        console.log(router);
        router.stateService.go(this.props.path);
        
    }
    componentWillUnmount() {
        ReactRouter.clear();
    }
    render() {
        ReactRouter.getRouter()['ngservices'] = this.props.ngservices;
        return (
            <div className="main-panel" id="read-main-panel">
                <div id="le-modal"></div>
                
                <LeModal store={store}  injectAsyncReducer={injectAsyncReducer} reduxstate={REDUX_STATE_MODAL}/>
                <UIRouter router={ReactRouter.getRouter()}>
                    <UIView name="header" />

                    <UIView name="summary"/>
                    <UIView name="subsummary" />
                    <div id="react-banner-container"></div>
                    <LeBanner store={store}  injectAsyncReducer={injectAsyncReducer} reduxstate={REDUX_STATE_BANNER} />
                    <UIView name="notice" />
                    <div className="main-body" id="react-main-body">
                        <UIView name="main"/>
                    </div>
                </UIRouter>
            </div>
        );
    }
}

angular
    .module("le.react.maincomponent", [])
    .component(
        "reactAngularMainComponent",
        react2angular(ReactAngularMainComponent, ['path', 'ngservices'], []));
