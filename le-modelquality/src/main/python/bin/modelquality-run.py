#!/usr/bin/env python

import os, sys

sys.path.append(os.path.join(os.path.dirname(__file__),'..'))

from lattice.modelquality.modelquality import main

if __name__ == '__main__':
    main()
