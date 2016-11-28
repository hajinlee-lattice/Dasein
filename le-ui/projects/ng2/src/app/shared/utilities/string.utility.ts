import { Injectable } from '@angular/core';

@Injectable()
export class StringUtility {

    constructor() { }

    IsEmptyString(stringToCheck:string): boolean {
        var isEmpty = true;
        if (stringToCheck != null && stringToCheck.trim() !== "") {
            isEmpty = false;
        }
        return isEmpty;
    }
    
    AddCommas(stringToChange:string): string {
        if (stringToChange == null) {
            return null;
        }
        var parts = stringToChange.toString().split(".");
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        return parts.join(".");
    }

    SubstituteAllSpecialCharsWithDashes(stringToChange:string): string {
        stringToChange = stringToChange.trim();
        return stringToChange.replace(/[^a-zA-Z0-9]/g, '-');
    }

    SubstituteAllSpecialCharsWithSpaces(stringToChange:string): string {
        stringToChange = stringToChange.trim();
        return stringToChange.replace(/[^a-zA-Z0-9]/g, ' ');
    }
    
}