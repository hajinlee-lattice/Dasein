import React, { Component, react2angular, UIRouter, UIView } from "common/react-vendor";
import ReactRouter from './router';
import './react-main.component.scss';

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

    render() {
        return (
            <div className="main-panel">
                <UIRouter router={ReactRouter.getRouter()}>
                    <UIView name="summary" />
                    <div className="main-body">
                        <UIView name="main" className="main-content"/>
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
