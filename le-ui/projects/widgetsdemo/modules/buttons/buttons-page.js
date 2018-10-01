import React, { Component } from '../../../common/react-vendor';
import LeButton from '../../../common/widgets/buttons/le-button';
import LeButtonActions from '../../../common/widgets/buttons/le-button-actions';

export default class ButtonsPage extends Component {

    constructor(props) {
        super(props);
        this.callbackHandler = this.callbackHandler.bind(this);
        this.state = {
            disabled: false
        }
        this.timeout;
    }

    callbackHandler(state) {
        this.setState({ disabled: true });
        this.tmpState = state;
        this.timeout = setTimeout(() => {
            this.setState({ disabled: false });
        }, 3000);
    }


    /** React lifecicles methods */

    componentWillUnmount() {
        clearTimeout(this.timeout)
    }

    render() {
        const iconConfig = {
            lable: "",
            classNames: ['button', 'borderless-button'],
            image: "fa fa-pencil-square-o"
        }
        const iconTitleConfig = {
            lable: "Click Me!",
            classNames: ['button', 'blue-button'],
            image: "fa fa-pencil-square-o"
        }
        const config = {
            lable: "Click",
            classNames: ['button', 'orange-button']
        }
        return (
            <div className="le-flex-v-panel">
                <h1>Buttons</h1>
                <div className="le-flex-h-panel">
                    <LeButton callback={this.callbackHandler} disabled={this.state.disabled} config={config} />
                    <LeButton lable="Click" config={iconConfig} callback={() => { alert('Test'); }} />
                    <LeButton lable="Click" config={iconTitleConfig} callback={() => { alert('Test'); }} />
                    <LeButtonActions />
                </div>
            </div>


        );
    }
}