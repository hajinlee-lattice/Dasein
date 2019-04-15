import React, { Component, react2angular, UIRouter, UIView } from "common/react-vendor";
import ReactRouter from './router';
import './react-main.component.scss';
import LeModal from 'common/widgets/modal/le-modal';
import { store, injectAsyncReducer } from 'store';

export default class ReactAngularMainComponent extends Component {
    constructor(props) {
        super(props);
        console.log(this.props.path);
    }


    componentDidMount() {
        console.log('ROUTER', ReactRouter);
        let router = ReactRouter.getRouter();
        console.log(router);
        router.stateService.go(this.props.path);
        
    }
    componentWillUnmount() {
        ReactRouter.clear();
    }
/* <div ui-view="header" id="mainHeaderView"></div>
                        <div ui-view="summary" id="mainSummaryView"></div>
                        <div ui-view="subsummary" id="subSummaryView"></div>
                        <div ui-view="banner" id="mainInfoView"></div>
                        <div ui-view="notice" id="mainNoticeView"></div>
                        <div ui-view="main" id="mainContentView"></div> */
    render() {
        return (
            <div className="main-panel">
                <LeModal store={store} opened={false} injectAsyncReducer={injectAsyncReducer} 
                callback={() =>{
                    console.log('MODAL CB');
                }} title="Org ID to Account ID Mapping" template={() => {
                    return (<p>Test</p>)
                }} />
                <UIRouter router={ReactRouter.getRouter()}>
                    <UIView name="header" />
                    <UIView name="summary" />
                    <UIView name="subsummary" />
                    <UIView name="banner" />
                    <UIView name="notice" />
                    <div className="main-body">
                        <UIView name="main" />
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
        react2angular(ReactAngularMainComponent, ['path'], []));
