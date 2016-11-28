/* tslint:disable:no-unused-variable */

import { TestBed, async, inject } from '@angular/core/testing';
import { StringsService } from './strings.service';

describe('Service: Strings', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StringsService]
    });
  });

  it('should ...', inject([StringsService], (service: StringsService) => {
    expect(service).toBeTruthy();
  }));
});
