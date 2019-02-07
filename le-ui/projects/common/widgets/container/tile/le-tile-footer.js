import React, { Component } from '../../../react-vendor';
import Aux from '../../hoc/_Aux';
import './le-tile.scss';
class LeTileFooter extends Component {
    constructor(props) {
        super(props);
    }
    render() {

        return (
            <Aux>
                <div className={`${"le-flex-h-panel space-around le-footer"} ${this.props.classNames ? this.props.classNames : ''}`}>
                    {this.props.children}
                </div>
            </Aux>

        );
    }
}

export default LeTileFooter;