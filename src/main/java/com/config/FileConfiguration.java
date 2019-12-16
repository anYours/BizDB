package com.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import java.io.File;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/12 13:00
 */
@Configuration
public class FileConfiguration extends WebMvcConfigurationSupport {

    @Value("${path.imgPath}")
    String imgPath;

    @Override
    protected void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/static/**").addResourceLocations(ResourceUtils.CLASSPATH_URL_PREFIX + "/static/");
        registry.addResourceHandler("/showImg/**")
                .addResourceLocations("file:"+imgPath + File.separatorChar);
        super.addResourceHandlers(registry);
    }
}
