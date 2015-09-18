package oim.tasks;

import oracle.iam.platform.Platform;
import oracle.iam.platform.utils.logging.SuperLogger;
import oracle.iam.scheduler.vo.StoppableTask;
import oracle.iam.scheduler.vo.TaskSupport;
import org.apache.commons.dbutils.QueryRunner;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Connects to the OIM data-source defined in the WebLogic Administration Console.
 * This code can be generalized to use any data-source defined in WebLogic Administration
 * Console. When supplying the the provider url (server instance url), make sure the data source is deployed
 * on the server instance. E.g. "oimOperationsDB" (jdbc/operationsDB) is deployed on the OIM and SOA instance.
 * You can use either the SOA or the OIM as the provider url.
 *
 * Troubleshooting:
 * Error:
 * Exception in thread "main" javax.naming.NoInitialContextException: Cannot 
 * instantiate class: weblogic.jndi.WLInitialContextFactorty [Root exception 
 * is java.lang.ClassNotFoundException: weblogic.jndi.WLInitialContextFactorty]
 * Fix:
 * Add "wlfullclient.jar" to the classpath of your project.
 *
 * Error:
 * Exception in thread "main" java.lang.UnsupportedOperationException: Remote JDBC disabled
 * Fix:
 * Modify the setDomainEnv.sh located in "/home/oracle/Oracle/Middleware/user_projects/domains/oim_domain/bin"
 * Find "WLS_JDBC_REMOTED_ENABLED" and change value.
 * WLS_JDBC_REMOTE_ENABLED="-Dweblogic.jdbc.remoteEnabled=true"
 * Execute the "setDomainEnv.sh" script and restart the WebLogic ADmin Server.
 *
 * Error:
 * Exception in thread "main" java.lang.NoClassDefFoundError: oracle/sql/BfileDBAccess
 * Fix:
 * Add "ojdbc6.jar" driver to the classpath of your project.
 *
 */
public class CleanException extends TaskSupport implements StoppableTask
{
    private final static Logger LOGGER = SuperLogger.getLogger(CleanException.class.getName());
    private static final String DELRECOEXCEPTION = "delete from recon_exceptions where rex_itres_key =";

    /**
     * Method description
     *
     *
     * @param attributes
     *
     * @throws Exception
     */
    public void execute(HashMap attributes) throws Exception {
        String methodName = "Execute";
        System.out.println("Entering "+ methodName);
        LOGGER.entering(getClass().getName(), methodName);
        List<String> result = null;
        String resourceKey = (String)attributes.get("IT Resource Key");

        QueryRunner run    = new QueryRunner(Platform.getNonXADataSource());
        try {
            System.out.println("executing query ..." + DELRECOEXCEPTION + resourceKey);
            run.update(DELRECOEXCEPTION + resourceKey);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "{0}", e);
        }
    }

    public HashMap getAttributes() {
        return null;
    }

    public void setAttributes() {
    }

    // Test Code
    public static void main(String[] args)
    {
        String webLogicContext="weblogic.jndi.WLInitialContextFactory"; //Name of the WebLogic Context
        String providerURL="t3://localhost:14000"; //OIM URL
        String dataSourceName = "jdbc/operationsDB"; //JNDI Name
        Connection connection = null; //connection to database
        Statement st = null;
        ResultSet rs = null;

        try
        {
            //Set your WebLogic Properties 
            Properties properties = new Properties();
            properties.put(Context.INITIAL_CONTEXT_FACTORY,webLogicContext);
            properties.put(Context.PROVIDER_URL, providerURL);

            Context context = new InitialContext(properties); //create the initial WebLogic Context 
            DataSource dataSource = (DataSource) context.lookup(dataSourceName); //lookup a datasource in WebLogic
            System.out.println("Lookup dataSource returned: " + dataSource);

            connection = dataSource.getConnection(); //Establish connection to the OIM database
            System.out.println("Connection to \"" + dataSourceName + "\" Established: " + connection.toString());

            //Query from the USR table
            st = connection.createStatement();
            String query = "SELECT * FROM USR";
            rs = st.executeQuery(query);

            while(rs.next())
            {
                String userLogin = rs.getString("usr_login");
                System.out.println(userLogin);
            }
        }
        catch (SQLException ex){
            Logger.getLogger(CleanException.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (NamingException ex){
            Logger.getLogger(CleanException.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally
        {
            if(rs != null)
            {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CleanException.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if(st != null)
            {
                try {
                    st.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CleanException.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if(connection != null)
            {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(CleanException.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }//end main
}//end class