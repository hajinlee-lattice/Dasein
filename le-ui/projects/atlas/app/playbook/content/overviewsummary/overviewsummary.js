import React, {
    Component,
    react2angular
} from "../../../../../common/react-vendor";
import "./overviewsummary.scss";
import Aux from "../../../../../common/widgets/hoc/_Aux";
import SummaryBox from "./overviewsummary-box";

import { store } from 'store';
import { actions } from '../../playbook.redux';


export default class OverviewSummaryContainer extends Component {
    constructor(props) {
        super(props);
        this.state = {
            loading: false,
            play: null
        };
    }

    componentWillUnmount() {
    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];
        this.state.play = playstore.play;
        this.setState(this.state);
    }

    makeSummaryBoxes(summaries) {
        var boxes = [];
        for(var i in summaries) {
            var name = i,
                body = summaries[i];
            if(body) {
                boxes.push(
                    <SummaryBox
                        name={name}
                        body={body}
                    />
                );
            }
        }
        return boxes;
    }

    render() {
        if(this.state.play) {
            let play = this.state.play;
            return (
                <Aux>
                    <div className="le-summary-container le-flex-v-panel">
                        <div className="le-summary-header le-flex-v-panel">
                            <p className="title">{play.displayName}</p>
                            <p className="description">
                                {play.description}
                            </p>
                        </div>
                        <div className="le-flex-h-panel boxes-container">
                            {this.makeSummaryBoxes({
                                Segment: play.targetSegment.display_name,
                                Accounts: play.targetSegment.accounts.toLocaleString(),
                                Contacts: play.targetSegment.contacts.toLocaleString(),
                                "Scoring Model": (play.ratingEngine ? play.ratingEngine.displayName : null)
                            })}
                        </div>
                    </div>
                </Aux>
            );
        } else {
            return (
                <Aux>
                    <p>Loading...</p>
                </Aux>
            );
        }
    }
}

angular
    .module("le.summary", [])
    .component("leSummaryComponent", react2angular(OverviewSummaryContainer, [], []));
