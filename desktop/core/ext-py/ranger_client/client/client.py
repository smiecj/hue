from typing import List
from bs4 import BeautifulSoup
import json

import logging
from api import Api

from model import Service, User

LOG = logging.getLogger(__name__)

# ranger client
## todo: set timeout: requests/adapters.py
class Client:
    
    TAG_USER = "vXUsers"
    TAG_NAME = "name"
    TAG_PASSWORD = "password"
    TAG_FIRST_NAME = "firstName"
    TAG_LAST_NAME = "lastName"
    
    PAGE_SIZE_DEFAULT = 50
    PAGE_SIZE_MAX = 100
    
    def __init__(self, address, user, password):
        self.api = Api(address, user, password)
        LOG.info("[Client] address: {}, api init finish".format(address))

    def get_all_users(self):
        ret = []
        user_count = self.api.get_user_count()
        # get each page user and return all user
        start = 0
        while start < user_count:
            get_user_content = self.api.get_user(start, self.PAGE_SIZE_DEFAULT)
            if get_user_content:
                current_user_list = self._parse_user(get_user_content)
                ret.extend(current_user_list)
            start += self.PAGE_SIZE_DEFAULT
        return ret
    
    # services will not be too mush, so get all services
    def get_all_service(self):
        service_content = self.api.get_service(0, self.PAGE_SIZE_MAX)
        return self._parse_service(service_content)
        
    def get_policy(self, service_id):
        policy_content = self.api.get_policy(service_id, 0, self.PAGE_SIZE_MAX)
        return self._parse_policy(policy_content)

    def add_user(self, name = "", password = "", first_name = "", last_name = ""):
        self.api.add_user(User(name, password, first_name, last_name))
        
    def update_policy(self, policy):
        self.api.update_policy(policy)

    def _parse_user(self, content):
        ret = []
        soup = BeautifulSoup(content, 'xml')
        for current_user in soup.find_all(self.TAG_USER):
            # LOG.info("[test] current user: {}, name: {}".format(current_user, current_user.find(self.TAG_NAME).get_text()))
            user_name = current_user.find(self.TAG_NAME).get_text()
            user_first_name = ""
            user_last_name = ""
            
            user_first_name_tag = current_user.find(self.TAG_FIRST_NAME)
            user_last_name_tag = current_user.find(self.TAG_LAST_NAME)
            if user_first_name_tag:
                user_first_name = user_first_name_tag.get_text()
            if user_last_name_tag:
                user_last_name = current_user.find(self.TAG_LAST_NAME).get_text()
            ret.append(User(name=user_name, first_name=user_first_name, last_name=user_last_name))
        return ret
    
    def _parse_service(self, content):
        ret = []
        service_obj = json.loads(content)
        for service in service_obj["services"]:
            ret.append(Service(service["id"], service["name"]))
        return ret
    
    def _parse_policy(self, content):
        return json.loads(content)
