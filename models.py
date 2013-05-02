from google.appengine.ext import ndb

class UserPost(ndb.Model):
    #id is auto generated
    user_id = ndb.StringProperty()
    post_id = ndb.StringProperty()
    commenters = ndb.StringProperty(repeated = True)
    likers = ndb.StringProperty(repeated = True)
class UserResult(ndb.Model):
    #id is user id
    self_likes = ndb.IntegerProperty()
    top_commenters = ndb.StringProperty(repeated = True)
    top_likers = ndb.StringProperty(repeated = True)
