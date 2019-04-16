import React, { Component } from '../../../react-vendor';
import Aux from '../../hoc/_Aux';
import './le-tile.scss';

class LeTile extends Component {
    constructor(props) {
        super(props);
    }
    render() {

        return (
            <Aux>
                <li className={`${"le-tile"} ${this.props.classNames ? this.props.classNames : ''}`}>
                    {this.props.children}
                </li>
            </Aux>
        );
    }
}

export default LeTile;
