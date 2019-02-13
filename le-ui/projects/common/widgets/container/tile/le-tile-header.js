import React, { Component } from '../../../react-vendor';
import Aux from '../../hoc/_Aux';
class LeTileHeader extends Component {
    constructor(props) {
        super(props);
    }
    render() {
        
        return (
            <Aux>
                <div className={`${"le-flex-h-panel space-middle le-header"} ${this.props.classNames ? this.props.classNames : ''}`}>
                    {this.props.children}
                </div>
            </Aux>

        );
    }
}

export default LeTileHeader;