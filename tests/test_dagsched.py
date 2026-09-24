import json
import threading
import unittest
import urllib.error
import urllib.request

from dagsched import Dag
from server import serve

class TestDag(unittest.TestCase):
    def test_add_counts(self):
        dag = Dag()
        self.assertEqual(dag.add("t1")["tasks"], 1)

    def test_first_ready(self):
        dag = Dag()
        dag.add("t1")
        self.assertEqual(dag.ready(), ["t1"])

    def test_run_marks_state(self):
        dag = Dag()
        dag.add("t1")
        self.assertEqual(dag.run("t1", True)["state"], "succeeded")

    def test_stats_shape(self):
        self.assertIn("max_retries", Dag().stats())

    def test_http_add_ready(self):
        server = serve(0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = "http://127.0.0.1:%d" % server.server_port
        urllib.request.urlopen(base + "/add", data=b'{"task": "t1", "deps": []}', timeout=5).read()
        with urllib.request.urlopen(base + "/ready", data=b"{}", timeout=5) as response:
            self.assertEqual(json.loads(response.read())["ready"], ["t1"])
        server.shutdown()
