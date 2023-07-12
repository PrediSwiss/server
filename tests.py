import unittest
from flask import Flask
from flask.testing import FlaskClient

from app import app

class AppTestCase(unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.app = app.test_client()

    def tearDown(self):
        pass

    def test_get_counter(self):
        response = self.app.get('/counter')
        self.assertEqual(response.status_code, 200)

    def test_get_current(self):
        response = self.app.get('/current')
        self.assertEqual(response.status_code, 200)