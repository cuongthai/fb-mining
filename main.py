import webapp2
from urls import routes
from config import config
import appengine_config

application = webapp2.WSGIApplication(routes=routes, debug=True, config=config)