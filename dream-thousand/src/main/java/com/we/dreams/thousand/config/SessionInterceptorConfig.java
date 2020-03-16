package com.we.dreams.thousand.config;

import com.we.dreams.thousand.interceptor.LoginInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.config
 * @date 2020/3/13 10:18
 * @description 资源拦截器
 */

@Configuration
public class SessionInterceptorConfig implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        //添加拦截器
        registry.addInterceptor(loginInterceptor())

                //添加不拦截的登录path
                .excludePathPatterns("/login")

                //处理favicon.ico图标请求问题
                .excludePathPatterns("/favicon.ico")

                //由于favicon.ico请求会出现找不到图标是404，引发Spring自身的error请求，需要排除error请求
                .excludePathPatterns("/error")

                //放行静态资源
                .excludePathPatterns("/frame/**")

                //添加拦截path
                .addPathPatterns("/**");
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/static/**").addResourceLocations("classpath:/static/");
        registry.addResourceHandler("/templates/**").addResourceLocations("classpath:/templates/");
    }

    @Bean
    public LoginInterceptor loginInterceptor(){
        return new LoginInterceptor();
    }


}
