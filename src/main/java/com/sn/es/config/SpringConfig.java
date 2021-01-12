package com.sn.es.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Created by nings on 2020/12/11.
 */
@Configuration
@PropertySource(value = {"classpath:es.properties"},ignoreResourceNotFound = true)
public class SpringConfig implements WebMvcConfigurer{

}
