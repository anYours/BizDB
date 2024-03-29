package com.common.util;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @Author wul
 * @Description
 * @Date 2019/12/11 15:52
 */
@Component
@ConfigurationProperties(prefix = "demo.test")
public class ConfigurationPropertiesConfig {

    private String name;
    private String des;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDes() {
        return des;
    }

    public void setDes(String des) {
        this.des = des;
    }

    @Override
    public String toString() {
        return "ConfigurationPropertiesConfig{" +
                "name='" + name + '\'' +
                ", des='" + des + '\'' +
                '}';
    }
}
