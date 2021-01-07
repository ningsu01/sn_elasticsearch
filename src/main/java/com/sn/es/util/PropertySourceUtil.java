package com.sn.es.util;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by nings on 2020/12/12.
 */
public class PropertySourceUtil {

    public static String getPropertyField(String field,String propertyName){
        Resource resource = new ClassPathResource(propertyName);
        try {
            Properties properties = PropertiesLoaderUtils.loadProperties(resource);
            return properties.getProperty(field);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
