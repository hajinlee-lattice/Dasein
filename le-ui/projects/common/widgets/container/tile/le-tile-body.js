import React, { Component } from '../../../react-vendor';
import Aux from '../../hoc/_Aux';
class LeTileBody extends Component {
    constructor(props) {
        super(props);
    }
    render() {

        return (
            <Aux>
                <div className={`${"le-flex-v-panel"} ${this.props.classNames ? this.props.classNames : ''}`}>
                    {this.props.children}
                </div>
            </Aux>

        );
    }
}

export default LeTileBody;