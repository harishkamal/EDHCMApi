package com.hca.edh.cm;

import com.cloudera.api.ApiRootResource;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.*;

import com.cloudera.api.v18.RootResourceV18;

import com.hca.edh.cm.util.AppConfig;
import com.hca.edh.cm.util.CLOptions;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
class App
{
    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static int exitStatus = 0;

    public static void main( String[] args )
    {
        try {
            System.out.print("Starting app...");
            System.out.println("Log4j configuration file: " + System.getProperty("log4j.configurationFile"));

            //Parse and Validate CommandLine arguments.
            CommandLine commandLineOptions = new CLOptions(args).parse();

            //Load the Application configuration from the config file into an Object.
            AppConfig appConfig = new AppConfig(new Properties(){{load(new FileInputStream(commandLineOptions.getOptionValue("configFile")));}});

            log.info("Application initialization is complete.");

            //Build a Root API Object
            ApiRootResource root = new ClouderaManagerClientBuilder()
                    .withBaseURL(new URL(appConfig.getCmURL()))
                    .disableTlsCertValidation()
                    .withUsernamePassword(appConfig.getUserName(), appConfig.getPassword())
                    .build();

            log.info("Successfully established the API connection for the version '" + root.getCurrentVersion()+"'!");
            RootResourceV18 rootResource = root.getRootV18();
            log.debug("Acquired the root resource.");

            //Get the HostID for the role config where the change need to be applied.
            //TODO : Use Stream API to simplify this further
            log.info("Getting the HostID for the agent host '"+appConfig.getAgentHost()+"'...");
            final String[] hostID = {null};
            rootResource.getHostsResource().readHosts(DataView.SUMMARY).forEach((ApiHost apiHost) ->
            {   log.debug(apiHost.getHostname()+" : "+apiHost.getHostId());
                if(apiHost.getHostname().equalsIgnoreCase(appConfig.getAgentHost())){
                    hostID[0] = apiHost.getHostId();
                    log.info("HostID '"+apiHost.getHostId()+"' found for the Agent Host '"+appConfig.getAgentHost()+"'.");
                }
             });
            if (hostID == null || hostID.length == 0)
                throw new Exception("No HostID found for the Agent Host '"+appConfig.getAgentHost()+"'");

            //Find the RoleConfigGroupName and the Role based on the HostID that's being found
            //TODO : Use Stream API to simplify this further
            log.info("Finding the RoleConfigGroupName and the Role based on the HostID that's being found..");
            String roleConfigGroupName = null, roleName = null;
            for (ApiRole apiRole : rootResource.getClustersResource().getServicesResource("cluster").getRolesResource("flume").readRoles()){
                log.info("Role Name : "+apiRole.getName() +
                        "\nHost ID : "+apiRole.getHostRef().getHostId() +
                        "\nRole Config Group Name : "+apiRole.getRoleConfigGroupRef().getRoleConfigGroupName() +
                        "\nRole state : "+apiRole.getRoleState());
                if(hostID[0].equalsIgnoreCase(apiRole.getHostRef().getHostId())) {
                    roleConfigGroupName = apiRole.getRoleConfigGroupRef().getRoleConfigGroupName();
                    roleName = apiRole.getName();
                    log.info("Found the RoleConfigGroupName and RoleName required based on the HostID!");
                }
            }

            //Get the current Flume configuration value
            log.info("Retrieving the current flume config value just as a backup...");
            String current_flume_config_val = null;
            for(ApiConfig apiConfig : rootResource.getClustersResource().getServicesResource(appConfig.getClusterName()).getRoleConfigGroupsResource(appConfig.getService()).readConfig(roleConfigGroupName, DataView.FULL).getConfigs()){
                log.trace(apiConfig.getName()+"|"+apiConfig.getValue()+"|"+apiConfig.getDefaultValue());
                if(apiConfig.getName().equalsIgnoreCase(appConfig.getFlumeFilePropertyName())) {
                    current_flume_config_val = apiConfig.getValue() != null ? apiConfig.getValue() : apiConfig.getDefaultValue();
                    if(apiConfig.getValidationState().compareTo(ApiConfig.ValidationState.OK) != 0)
                        throw new Exception("Current configuration value '"+current_flume_config_val+"' is not valid. Please correct this manually and run this program again. (Validation Message = '"+apiConfig.getValidationMessage()+"' ; Validation State = '"+apiConfig.getValidationState()+"'");
                }
            }
            log.info("Current flume config value retrieval successful.");

            //Update the Flume configuration
            log.info("Attempting to update flume configuration...");
           // new String(Files.readAllBytes(Paths.get(commandLineOptions.getOptionValue("flumeConfigFile"))));
          if(updateFlumeConfig(rootResource, appConfig, roleConfigGroupName, new String(Files.readAllBytes(Paths.get(commandLineOptions.getOptionValue("flumeConfigFile")))))){
                String role = roleName;
              //Restart the role, since the update is successful.
               for(ApiCommand apiCommand : rootResource.getClustersResource().getServicesResource(appConfig.getClusterName()).getRoleCommandsResource(appConfig.getService()).restartCommand(new ApiRoleNameList(){{add(role);}})){
                   log.info("Restart command submitted for the role '"+role+"'.......");
                   //Keep checking every second to see if the restart completed.
                   while(apiCommand.isActive()){
                       TimeUnit.SECONDS.sleep(1);
                       apiCommand = rootResource.getCommandsResource().readCommand(apiCommand.getId());
                   }
                   log.info("Role restart {} with message ", apiCommand.getSuccess() ? "succeeded" : "failed" + apiCommand.getResultMessage());
                   if(!apiCommand.getSuccess())
                       exitStatus = 1;
               }

          }else{
              log.info("Reverting back to the old configuration...");
              updateFlumeConfig(rootResource, appConfig, roleConfigGroupName, current_flume_config_val);
              exitStatus = 1;
          }

        }catch(Exception ex){
            log.error("An error occurred when applying changes in the flume config", ex);
            exitStatus = 1;
        }
        System.exit(exitStatus);
    }

    private static boolean updateFlumeConfig(RootResourceV18 rootResource, AppConfig appConfig, String roleConfigGroupName, String flumeConfigValue){
        boolean success = true;
        try {
            rootResource.getClustersResource().getServicesResource(appConfig.getClusterName()).getRoleConfigGroupsResource(appConfig.getService()).updateConfig(roleConfigGroupName, appConfig.getApiMessage(), new ApiConfigList() {{
                add(new ApiConfig(appConfig.getFlumeFilePropertyName(), flumeConfigValue));
            }});

            for(ApiConfig apiConfig : rootResource.getClustersResource().getServicesResource(appConfig.getClusterName()).getRoleConfigGroupsResource(appConfig.getService()).readConfig(roleConfigGroupName, DataView.FULL).getConfigs()) {
                if(apiConfig.getName().equalsIgnoreCase(appConfig.getFlumeFilePropertyName())) {
                    log.debug("Config name : " + apiConfig.getName() + " \nValue : " + apiConfig.getValue() + "\nValidation State : "+apiConfig.getValidationState() +"\nValidation message : '"+apiConfig.getValidationMessage()+"'");
                    if (apiConfig.getValidationState().compareTo(ApiConfig.ValidationState.OK) != 0) {
                        log.error("Flume config update unsuccessful :( :(");
                        log.error("Updated Agent configuration file with validation state '" + apiConfig.getValidationState() + "' and validation message '" + apiConfig.getValidationMessage() + "'.");
                        success = false;
                    } else
                        log.info("Flume configuration updated for the config name '"+apiConfig.getName()+"'!");
                }
            }
        }catch(Exception ex){
            log.error("Error occurred when trying to update flume configuration", ex); success = false;
        }
        return success;
    }
}