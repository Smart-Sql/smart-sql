package org.gridgain.superservice;

import org.apache.ignite.Ignite;

public interface ISmartFuncInit {

    public Object getUserGroup(Ignite ignite, Object group_id, String user_token);

    public Boolean hasUserTokenType(String user_token_type);

    public Object getUserToken(Ignite ignite, String group_name);

    public Object addUserGroup(Ignite ignite, Object group_id, String group_name, String user_token, String group_type, String schema_name);

    public Object updateUserGroup(Ignite ignite, Object group_id, String group_name, String group_type);

    public Object deleteUserGroup(Ignite ignite, Object group_id, String group_name);
}
