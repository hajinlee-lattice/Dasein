import React, { Component } from '../../../common/react-vendor';
import LeButton from '../../../common/widgets/buttons/le-button';

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

        const config = {
            lable: "",
            classNames: ['button', 'orange-button']
        }
        return (
            <div>
                <h1>Buttons</h1>
                <LeButton lable="Click" callback={this.callbackHandler} disabled={this.state.disabled} config={config} />
            </div>


        );
    }
}