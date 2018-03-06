package com.inspur.examples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class KafkaProperties
{
    private static final Log LOG =  LogFactory.getLog(KafkaProperties.class);
    
    // Topic名称，安全模式下，需要以管理员用户添加当前用户的访问权限
    final static String topic = "kelvin1";
    
    private static Properties serverProps = new Properties();
    private static Properties producerProps = new Properties();
    
    private static Properties consumerProps = new Properties();
    
    private static Properties clientProps = new Properties();
    
    private static KafkaProperties instance = null;

    private void loadConfig(Properties Config, String filename) {
        InputStream in;
        in =  FileUtils.class.getClassLoader().getResourceAsStream(filename);
        try {
            Config.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private KafkaProperties()
    {
        loadConfig(producerProps, "conf/producer.properties");
        loadConfig(consumerProps, "conf/consumer.properties");
        loadConfig(serverProps, "conf/server.properties");
        loadConfig(clientProps, "conf/client.properties");
    }
    
    synchronized static KafkaProperties getInstance()
    {
        if (null == instance)
        {
            instance = new KafkaProperties();
        }
        
        return instance;
    }
    
    /**
    * 获取参数值
    * @param key properites的key值
    * @param defValue 默认值
    * @return Property Value
    */
    String getValues(String key, String defValue)
    {
        String rtValue = null;
        
        if (null == key)
        {
            LOG.error("key is null");
        }
        else
        {
            rtValue = getPropertiesValue(key);
        }
        
        if (null == rtValue)
        {
            LOG.warn("KafkaProperties.getValues return null, key is " + key);
            rtValue = defValue;
        }
        
        LOG.info("KafkaProperties.getValues: key is " + key + "; Value is " + rtValue);
        
        return rtValue;
    }
    
    /**
    * 根据key值获取server.properties的值
    * @param key 属性名称
    * @return value 属性值
    */
    private String getPropertiesValue(String key)
    {
        String rtValue = serverProps.getProperty(key);
        
        // server.properties中没有，则再向producer.properties中获取
        if (null == rtValue)
        {
            rtValue = producerProps.getProperty(key);
        }
        
        // producer中没有，则再向consumer.properties中获取
        if (null == rtValue)
        {
            rtValue = consumerProps.getProperty(key);
        }
        
        // consumer没有，则再向client.properties中获取
        if (null == rtValue)
        {
        	rtValue = clientProps.getProperty(key);
        }
        
        return rtValue;
    }
}
