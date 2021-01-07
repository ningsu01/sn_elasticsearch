package com.sn.es.config.swagger;

import io.swagger.annotations.Api;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Created by nings on 2020/12/8.
 */
@Configuration
@EnableSwagger2
@ComponentScan(value = {"com.sn.es.controller"})
public class SwaggerConfig {

    @Bean
    public Docket docket(){
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .paths(PathSelectors.any())
                .apis(RequestHandlerSelectors.withClassAnnotation(Api.class))
                .build();
    }

    public ApiInfo apiInfo(){
        return new ApiInfoBuilder().title("api接口文档")
                .description("接口说明")
                .termsOfServiceUrl("http://localhost:8082/sn_es")
                .contact(new Contact("suning","","2897723319@qq.com"))
                .version("1.0.0")
                .build();
    }



}
