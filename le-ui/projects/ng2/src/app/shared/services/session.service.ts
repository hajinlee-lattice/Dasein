import { Injectable } from '@angular/core';
import { StringsUtility } from '../utilities/strings.utility';
import { StorageUtility } from '../utilities/storage.utility';

@Injectable()
export class SessionService {

    constructor(
        private stringsUtility: StringsUtility,
        private storageUtility: StorageUtility
    ) { }

    public ClearSession(): void {
        this.storageUtility.clear(false);
        this.stringsUtility.clearResourceStrings();
        window.location.reload();
    }
    
    public HandleResponseErrors(data, status): void {
        if (status === 401) {
            this.ClearSession();
        }
    }

}
