import webapp2
import logging
import os
from google.appengine.ext.webapp import template


class IndexHandler(webapp2.RequestHandler):
	def get(self):
		path = os.path.join(os.path.dirname(__file__), '../templates/mapreduce.html')

		self.response.out.write(template.render(path, {}))

