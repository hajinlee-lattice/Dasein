import React, { Component } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
//import { actions, reducer } from '../../playbook.redux';
import { get } from 'lodash';

class MainComponent extends Component {
    constructor(props) {
        super(props);
    }

    getTalkingPointsCount() {
        if(_.get(this.props, 'play.talkingPoints')) {
            return this.props.play.talkingPoints.length;
        }
        return 0;
    }

    getTargetSegmentsCounts() {
        var counts = {
            accounts: 0,
            contacts: 0
        };
        if(_.get(this.props, 'play.targetSegment')) {
            if(this.props.play.targetSegment.accounts) {
                counts.accounts = this.props.play.targetSegment.accounts;
            }
            if(this.props.play.targetSegment.contacts) {
                counts.contacts = this.props.play.targetSegment.contacts;
            }
        }
        return counts;
    }

    render() {
        return (
            <LeVPanel flex={"1"} className="main-panel panel">
                <ul>
                    <li>
                        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed in tellus sagittis, posuere leo vel, pellentesque tortor. Sed posuere massa nibh, sit amet pharetra elit suscipit vitae. Nulla ornare pharetra purus, ac feugiat ante dictum et. Donec mollis aliquet lacinia.
                    </li>
                    <li>
                        {this.getTalkingPointsCount()} talking points have been created.
                    </li>
                    <li>
                        {this.getTargetSegmentsCounts().accounts || this.getTargetSegmentsCounts().contacts ? <h2>Avaliable to Send</h2> : null}
                        <LeHPanel>
                            {this.getTargetSegmentsCounts().accounts ? 
                            <div>
                                <h3>{this.getTargetSegmentsCounts().accounts.toLocaleString()}</h3>
                                Accounts
                            </div>
                            : null}
                            {this.getTargetSegmentsCounts().contacts ? 
                            <div>
                                <h3>{this.getTargetSegmentsCounts().contacts.toLocaleString()}</h3>
                                Contacts
                            </div>
                            : null}
                        </LeHPanel>
                    </li>
                </ul>
            </LeVPanel>
        );
    }
}
export default MainComponent;