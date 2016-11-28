/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { StringsUtility } from './strings.utility';

describe('Service: StringsUtility', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StringsUtility]
    });
  });

  it('should ...', inject([StringsUtility], (service: StringsUtility) => {
    expect(service).toBeTruthy();
  }));
});
