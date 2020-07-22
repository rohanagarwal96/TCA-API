'''Access OneTick data via pandas as a service - this module forwards calls to the server'''
from .query import CLOUD_URL

class Config:
    API_KEY = None 
    user = None
    password = None
    config=None
    url=None

    @classmethod
    def session(cls, username, password, url=CLOUD_URL, config=r'..\config\cirrus.cfg'):
        Config.user = username
        Config.password = password
        Config.config = config
        Config.url = url      
