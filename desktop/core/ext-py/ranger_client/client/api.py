import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup

class Api:
    PATH_LOGIN = "/j_spring_security_check"
    PATH_LIST_USER = "/service/xusers/users"
    PATH_ADD_USER = "/service/xusers/secure/users"
    PATH_LIST_SERVICES = "/service/plugins/services"
    PATH_LIST_POLICIES = "/service/plugins/policies/service/{}"
    PATH_UPDATE_POLICY = "/service/plugins/policies/{}"

    ERR_LOGIN_FAILED = "login failed"
    ERR_ADD_USER_FAILED = "add user failed"
    ERR_UPDATE_POLICY_FAILED = "update policy failed"

    PAGE_SIZE_MINIMAL = 1
    
    def __init__(self, address, user, password):
        self.address = address
        self.user = user
        self.password = password
        # no need login (to avoid login cost)
        """ login_ret = requests.post(self.address + self.PATH_LOGIN, data={"j_username": self.user, "j_password": self.password})
        if login_ret.status_code != 200:
            raise ValueError(self.ERR_LOGIN_FAILED + ": code = {}".format(login_ret.status_code)) """
       
    # get ranger api basic auth
    def _get_auth(self):
        return HTTPBasicAuth(self.user, self.password)
     
    # get ranger user content (xml)
    def get_user(self, start, page_size):
        get_user_first_page_ret = requests.get(self.address + self.PATH_LIST_USER, auth=self._get_auth(), params={"pageSize": page_size, "startIndex": start})
        if get_user_first_page_ret.status_code == 200:
            return get_user_first_page_ret.content
        return ""
    
    # get ranger user count
    def get_user_count(self):
        get_user_first_page_ret = requests.get(self.address + self.PATH_LIST_USER, auth=self._get_auth(), params={"pageSize": self.PAGE_SIZE_MINIMAL, "startIndex": 0})
        if get_user_first_page_ret.status_code == 200:
            soup = BeautifulSoup(get_user_first_page_ret.content, 'xml')
            totalCount = soup.find("totalCount")
            return int(totalCount.get_text())
        return 0
    
    # add user
    def add_user(self, user):
        add_user_ret = requests.post(self.address + self.PATH_ADD_USER, auth=self._get_auth(), json={
            "name": user.name,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "password": user.password,
            "userRoleList": [
                "ROLE_USER"
            ],
            "status": 1,
            "isVisible": 1
        })
        if add_user_ret.status_code != 200:
            raise ValueError(self.ERR_ADD_USER_FAILED + ": code = {}".format(add_user_ret.status_code))
    
    # get services
    def get_service(self, start, page_size):
        get_service_ret = requests.get(self.address + self.PATH_LIST_SERVICES, auth=self._get_auth(), params={"pageSize": page_size, "startIndex": start})
        if get_service_ret.status_code == 200:
            return get_service_ret.content
        return ""
    
    ## get policy
    def get_policy(self, service_id, start, page_size):
        get_policy_ret = requests.get(self.address + self.PATH_LIST_POLICIES.format(service_id), auth=self._get_auth(), params={"pageSize": page_size, "startIndex": start})
        if get_policy_ret.status_code == 200:
            return get_policy_ret.content
        return ""
    
    # modify policy
    def update_policy(self, policy):
        update_policy_ret = requests.put(self.address + self.PATH_UPDATE_POLICY.format(policy["id"]), auth=self._get_auth(), json=policy)
        if update_policy_ret.status_code != 200:
            raise ValueError(self.ERR_ADD_USER_FAILED + ": code = {}".format(update_policy_ret.status_code))