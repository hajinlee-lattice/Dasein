import React, { Component } from "../../../../../common/react-vendor";
import "./overviewsummary-box.scss";

export default class OverviewSummaryBox extends Component {
    constructor(props) {
        super(props);
    }

    componentDidMount() {}
    render() {
        return (
            <div className="le-flex-h-panel flex-content">
                <div className="le-summary-box le-flex-v-panel">
                    <div className="title">
                        {this.props.name}
                    </div>
                    <div className="body">
                        {this.props.body}
                    </div>
                </div>
            </div>
        );
    }
}
