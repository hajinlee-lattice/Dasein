import React, { Component } from "common/react-vendor";
import LeVPanel from 'common/widgets/container/le-v-panel.js'
import './priorities-list.component.scss';

export default class PrioritiesLitComponent extends Component {
    constructor(props) {
        super(props);
    }

    componentDidMount() {

    }
    componentWillUnmount() {

    }


    render() {
        return (

            <LeVPanel hstretch={"true"} className="priorities-list">
                The box here
            </LeVPanel>

        );
    }
}
