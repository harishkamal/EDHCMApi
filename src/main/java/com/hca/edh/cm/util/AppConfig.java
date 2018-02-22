package com.hca.edh.cm.util;

import java.util.Properties;

/**
 * This class contains the necessary Application Configuration properties.
 */
public class AppConfig {

    private String clusterName;
    private String service;
    private String cmURL;
    private String agentHost;
    private String userName;
    private String password;
    private String flumeFilePropertyName;
    private String apiMessage;

    public AppConfig(Properties properties) throws Exception{
        setClusterName(properties.getProperty("clusterName"));
        setService(properties.getProperty("service"));
        setCmURL(properties.getProperty("cmURL"));
        setAgentHost(properties.getProperty("agentHost"));
        setUserName(properties.getProperty("userName"));
        setPassword(properties.getProperty("password"));
        setFlumeFilePropertyName(properties.getProperty("flumeFilePropertyName"));
        setApiMessage(properties.getProperty("apiMessage"));
        if(getClusterName() == null || getService() == null || getCmURL() == null
                || getAgentHost() == null || getUserName() == null || getPassword() == null
                || getFlumeFilePropertyName() == null || getApiMessage() == null)
            throw new Exception("Required configuration is missing. Please check properties file to ensure all the required configuration is setup.");
    }

    public String getApiMessage() {
        return apiMessage;
    }

    private void setApiMessage(String apiMessage) {
        this.apiMessage = apiMessage;
    }

    public String getFlumeFilePropertyName() {
        return flumeFilePropertyName;
    }

    private void setFlumeFilePropertyName(String flumeFilePropertyName) {
        this.flumeFilePropertyName = flumeFilePropertyName;
    }

    public String getUserName() {
        return userName;
    }

    private void setUserName(String userName) {
        this.userName = userName;
    }

    public String getClusterName() {
        return clusterName;
    }

    private void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getService() {
        return service;
    }

    private void setService(String service) {
        this.service = service;
    }

    public String getCmURL() {
        return cmURL;
    }

    private void setCmURL(String cmURL) {
        this.cmURL = cmURL;
    }

    public String getAgentHost() {
        return agentHost;
    }

    private void setAgentHost(String agentHost) {
        this.agentHost = agentHost;
    }

    public String getPassword() {
        return password;
    }

    private void setPassword(String password) {
        this.password = password;
    }

}