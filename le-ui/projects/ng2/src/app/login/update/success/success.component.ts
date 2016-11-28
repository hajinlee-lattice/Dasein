import { Component } from '@angular/core';
import { LoginService } from '../../../shared/services/login.service';
import { StringsUtility } from "../../../shared/utilities/strings.utility";

@Component({
  selector: 'app-success',
  templateUrl: './success.component.html',
  styleUrls: ['./success.component.scss']
})
export class SuccessComponent {

    public strings: StringsUtility;

    constructor(
        private loginService: LoginService,
        private stringsUtility: StringsUtility
    ) { 
        this.strings = stringsUtility;
    }

    clickRelogin(): void {
        this.loginService.Logout();
    }
}
