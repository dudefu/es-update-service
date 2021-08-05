package xinyi.info.xinyi_es_service.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils
{
    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);

    private static Properties props;

    static
    {
        loadProperties();
    }

    synchronized static private void loadProperties()
    {
        logger.info("开始加载properties文件内容.......");
        props = new Properties();

        InputStream in = null;

        try
        {
            in = PropertiesUtils.class.getClassLoader().getResourceAsStream("application.properties");
            props.load(in);
        }
        catch (FileNotFoundException e)
        {
            logger.error("application.properties文件未找到");
        }
        catch (IOException e)
        {
            logger.error("出现IOException");
        }
        finally
        {
            try
            {
                if (null != in)
                {
                    in.close();
                }
            }
            catch (IOException e)
            {
                logger.error("application.properties文件流关闭出现异常");
            }
        }
        logger.info("加载properties文件内容完成...........");
        logger.info("properties文件内容：" + props);
    }

    public static String getString(String key)
    {
        if (null == props)
        {
            loadProperties();
        }
        return props.getProperty(key);
    }

    public static String getString(String key, String defaultValue)
    {
        if (null == props)
        {
            loadProperties();
        }
        return props.getProperty(key, defaultValue);
    }
}
