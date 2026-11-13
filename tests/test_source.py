import json
import threading
import unittest
from urllib.request import urlopen
from src import source

class SupplierSourceTests(unittest.TestCase):
    def setUp(self): self.source = source.Source()
    def route(self, path, headers=None): return self.source.route('GET', path, headers or {}, b'')
    def test_protocol(self):
        self.assertEqual(self.route('/busy')[0], 429)
        self.assertEqual(self.route('/busy')[1]['Retry-After'], '5')
        self.source.state['busy'] = False
        self.assertEqual(self.route('/busy')[0], 200)
        self.assertIn('Allow: /private/open$', self.route('/robots.txt')[2].decode())
    def test_http_state_endpoint(self):
        with source.make_server() as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            try:
                with urlopen('http://127.0.0.1:%d/__state' % server.server_port, timeout=5) as response:
                    self.assertEqual(json.load(response)['generation'], 1)
            finally:
                server.shutdown(); thread.join()

if __name__ == '__main__': unittest.main()
