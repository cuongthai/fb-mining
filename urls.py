# -*- coding: utf-8 -*-
"""URL definitions."""
import webapp2

routes = [
        (r'/','controllers.facebookoauth.HomeHandler'),
        (r'/send/','controllers.facebookoauth.SendHandler'),
        (r'/auth/login','controllers.facebookoauth.LoginHandler'),
        (r'/auth/logout','controllers.facebookoauth.LogoutHandler'),
        (r'/worker/fetch/posts/','workers.WorkerHandler.FBPostFetcher'),
        (r'/admin/mapreduce/','mapreduce_handler.MapReduceHandler'),
        (r'/admin/mapreduce/upload/','mapreduce_handler.UploadHandler'),
        (r'/admin/mapreduce/download/(.*)','mapreduce_handler.DownloadHandler'),
        

	
]

