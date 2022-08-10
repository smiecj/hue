# -*- coding: UTF-8 -*-
import logging

LOG = logging.getLogger(__name__)

# job
class Job:
    
    RET_SUCCESS = "success"
    RET_SERVICE_NOT_EXISTS = "service not exists"
    RET_POLICY_NOT_VALID = "policy obj is not valid"
    RET_POLICY_NOT_EXISTS = "policy not exists"

    def __init__(self, client):
        self.client = client
        
    # 添加新用户和默认权限
    def save_user_to_policy(self, user_name, user_password, service_name, policy_name, match_user_name):
        ## 添加用户
        ## 简化逻辑，如果接口层出现问题直接抛出 error ，job 逻辑层不进行额外处理
        self._add_new_user(user_name, user_password)
        
        ## 匹配 service
        service_id = self._get_service_id_by_name(service_name)
        if not service_id:
            raise ValueError(self.RET_SERVICE_NOT_EXISTS)
        
        ## 匹配 policy
        policy_obj = self.client.get_policy(service_id)
        if not policy_obj or not policy_obj["policies"]:
            raise ValueError(self.RET_POLICY_NOT_VALID)
        match_policy = False
        need_update = False
        to_update_policy = None
        for current_policy in policy_obj["policies"]:
            if current_policy["name"] == policy_name:
                match_policy = True
                ### 匹配 policy item
                for current_policy_item in current_policy["policyItems"]:
                    if match_user_name in current_policy_item["users"] and user_name not in current_policy_item["users"]:
                        need_update = True
                        to_update_policy = current_policy
                        current_policy_item["users"].append(user_name)
                        break
                break
        if not match_policy:
            raise ValueError(self.RET_POLICY_NOT_EXISTS)
        if need_update:
            self.client.update_policy(to_update_policy)
        else:
            LOG.info("[ranger.job] no need update user: {}".format(user_name))
    
    # _add_new_user: 查询所有用户，确定用户是否已在 ranger 存在，没有的话 需要先新建用户
    def _add_new_user(self, user_name, user_password):
        user_arr = self.client.get_all_users()
        LOG.info("[ranger] user count: %d", len(user_arr))
        exists = False
        for current_user in user_arr:
            if current_user.name == user_name:
                exists = True
        if not exists:
            # first name 和 last name 默认和用户名一致
            self.client.add_user(user_name, user_password, user_name, user_name)
    
    ## _get_service_id_by_name: 通过服务id 获取对应服务（如: hive）
    def _get_service_id_by_name(self, service_name):
        service_arr = self.client.get_all_service()
        LOG.info("[ranger] service count: %d", len(service_arr))
        for current_service in service_arr:
            if service_name == current_service.name:
                return current_service.id
        return None
