package com.we.dreams.thousand.config;

import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.config
 * @date 2020/3/12 11:18
 * @description mybatis plus config
 */
@EnableTransactionManagement //开启事务支持
@Configuration
public class MybatisPlusConfig {

    @Bean
    public PaginationInterceptor paginationInterceptor(){

        PaginationInterceptor paginationInterceptor = new PaginationInterceptor();

        //设置请求页超过最大页后的操作， true调回首页， false继续请求， 默认false
        paginationInterceptor.setOverflow(true);
        return  paginationInterceptor;
    }

}
