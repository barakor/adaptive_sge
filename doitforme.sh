#!/bin/bash

python $(dirname "${BASH_SOURCE[0]}")/SGE_adaptive.py --host math102-lx.wisdom.weizmann.ac.il --port 4693 --bokeh-port 4691 --http-port 4692 --nthreads 1
