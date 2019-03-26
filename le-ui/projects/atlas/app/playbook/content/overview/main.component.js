import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import { actions, reducer } from '../../playbook.redux';

class MainComponent extends Component {
    constructor(props) {
        super(props);
    }

    render() {
        return (
            <LeVPanel flex={"1"}>
                <ul>
                    <li>
                        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed in tellus sagittis, posuere leo vel, pellentesque tortor. Sed posuere massa nibh, sit amet pharetra elit suscipit vitae. Nulla ornare pharetra purus, ac feugiat ante dictum et. Donec mollis aliquet lacinia.
                    </li>
                    <li>
                        37 talking points have been created.
                    </li>
                    <li>
                        <h2>Avaliable to Send</h2>
                        <LeHPanel>
                            <div>
                                <h3>2,122</h3>
                                Accounts
                            </div>
                            <div>
                                <h3>3,445</h3>
                                Contacts
                            </div>
                        </LeHPanel>
                    </li>
                </ul>
            </LeVPanel>
        );
    }
}
export default MainComponent;