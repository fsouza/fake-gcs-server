# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

./fake-gcs-server -backend memory -data $PWD/examples/data &

pip install -r examples/python/requirements.txt
python examples/python/python.py
