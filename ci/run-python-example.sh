# Copyright 2019 Francisco Souza. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

./fake-gcs-server &

EXTERNAL_URL=https://localhost:4443
PUBLIC_HOST=https://localhost:4443

pip install -r examples/python/requirements.txt
python examples/python/python.py
