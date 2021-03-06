import webapp2
from models import UserPost,UserResult

import logging
from google.appengine.api import urlfetch
from google.appengine.api import channel
import urllib
import json
from google.appengine.ext import ndb
import time
from google.appengine.api import taskqueue

f_round = 5
class FBPostFetcher(webapp2.RequestHandler):
    @ndb.toplevel
    def get(self):
        logging.info("Running task")
        access_token = self.request.get("access_token")
        user_id = self.request.get("user_id")
        url = "https://graph.facebook.com/me/posts/?fields=message,link,likes,type,comments.fields(from)&limit=200&" + urllib.urlencode(dict(access_token=access_token))
        logging.info('access token %s'%access_token)
        self_likes = 0
        for i in range(f_round):
            logging.info("before fetch %s %s"%(i,int(round(time.time() * 1000))))
            channel.send_message(user_id,json.dumps({"status":"Fetching Posts..."}))
            result = urlfetch.fetch(url,deadline=120)
            channel.send_message(user_id,json.dumps({"status":"Calculating..."}))
            logging.info("after fetch %s %s"%(i,int(round(time.time() * 1000))))
            if result.status_code == 200:
                data = None
                try:
                    data = json.loads(result.content)
                except:
                    break
                posts = data.get('data')
            
                paging = data.get('paging')
                next = paging.get('next') if paging else None
                logging.info("number of posts %s "%len(posts))
                logging.info("next %s "%next)
                
                model_trunks = [] 
                for post in posts:
                    if post.get('message'):
                        message = post.get('message')
                        id = post['id']
                        if post['type']!='status':
                            continue
                        comments = post.get('comments') if post.get('comments') else {}
                        likes = post.get('likes') if post.get('likes') else {}
                        commenters = []
                        likers = []
                        if comments:
                            commenters = [c['from']['id'] for c in comments['data'] if c and c['from']] 
                        if likes:
                            likers = [l['id'] for l in likes['data'] if l and l['id']]
                            num_likes = likers.count(user_id)
                            self_likes = self_likes + num_likes
                            if num_likes:
                                channel.send_message(user_id,json.dumps({"status":"This link %s"%post.get('link')}))
                                logging.info(id)
                        
                        model_trunks.append(UserPost(id='%s_%s'%(user_id,id),user_id = user_id,post_id = id,commenters = commenters,likers = likers))
                        #logging.debug('%s %s => %s %s'%(id, message,len(commenters),len(likers)))
                logging.info("before put %s %s"%(i,int(round(time.time() * 1000))))
                #Do async in trunks
                ndb.put_multi_async(model_trunks)
                logging.info("after put %s %s"%(i,int(round(time.time() * 1000))))
                


                
                if not next:
                    break
                url = next

                self.response.out.write("DONE")
            else:
                self.response.out.write("ERROR")
                break
        #Save user report
        userResult = ndb.Key(UserResult,user_id).get()
        if not userResult:
            UserResult(id=user_id,self_likes=self_likes).put()
        else:
            userResult.self_likes = self_likes
        #notify client
        channel.send_message(user_id,json.dumps({"status":"OK","data":str(self_likes)}))
        #taskqueue.add(url='/admin/mapreduce/',params={},method='GET')
