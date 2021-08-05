package xinyi.info.xinyi_es_service.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import xinyi.info.xinyi_es_service.utils.PropertiesUtils;

import java.net.InetSocketAddress;


/**
 * 
 * ES客户端工具类
 * @author XIVA
 */
public class EsClientFactory
{
    /**
     * 是否扫描集群
     */
    private static boolean sniff = false;
    
    /**
     * ES 集群名称
     */
    private static String clusterName;
    
    /**
     * IP地址
     */
    private static String[] ipPorts;

    /**
     * ES 客户端对象
     */
    private static TransportClient esClient;
    
    public synchronized static TransportClient getInstance()
    {
        if (esClient == null)
        {
            clusterName = PropertiesUtils.getString("es.cluster.name", "LGFJ_XCLOUD_CLUSTER");
            
            ipPorts = PropertiesUtils.getString("es.cluster.hostname", "10.24.5.34:9300").split(",");
            
            try
            {
                init();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            
        }
        
        return esClient;
    }
    
    public static void closeClient()
    {
       /* try
        {
            if (esClient != null)
            {
                esClient.close();
            } 
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            esClient = null;
        }*/
    }

    public EsClientFactory(String clusterName, String[] ipPorts)
    {
    }
    public EsClientFactory(){
    }

    /**
     * ES 客户端连接初始化
     * 
     * @return ES客户端对象
     */
    private static void init()
    {
        // 创建集群client并添加集群节点地址
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        
        // 这里可以同时连接集群的服务器,可以多个,并且连接服务是可访问的
        esClient = new TransportClient(settings);
        
        for (String ipPort : ipPorts)
        {
            String[] ipPortArray = ipPort.split(":");
            String esIp = ipPortArray[0];
            int esPort = Integer.valueOf(ipPortArray[1]);
            
            esClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(esIp, esPort)));
        }
    }

    public TransportClient getEsClient()
    {
        return esClient;
    }

    public boolean isSniff()
    {
        return sniff;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public static String[] getIpPorts()
    {
        return ipPorts;
    }

    public static void setIpPorts(String[] ipPorts)
    {
        EsClientFactory.ipPorts = ipPorts;
    }
   
}
