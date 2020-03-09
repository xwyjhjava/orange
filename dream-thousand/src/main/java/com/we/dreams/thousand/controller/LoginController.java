package com.we.dreams.thousand.controller;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.we.dreams.thousand.model.TbUser;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.controller
 * @date 2020/3/9 13:37
 * @description 登录
 */
@Controller
public class LoginController {

    private static final Logger LOG = LoggerFactory.getLogger(LoginController.class);

    @PostMapping("/login")
    @ResponseBody
    public String login(@Param("username") String username, @Param("password") String password){

        LOG.info("input username : {}", username );
        LOG.info("input password : {}", password);

        TbUser user = new TbUser();
        //省略MD5
        QueryWrapper<TbUser> wrapper = new QueryWrapper<>();
        wrapper.eq("user_name", username)
               .eq("password", password);
        TbUser tbUser = user.selectOne(wrapper);
        if(tbUser != null){
            LOG.info("db username => {}", tbUser.getUserName());
            LOG.info("db password => {}", tbUser.getPassword());
            return "login success";
        }
        return "login failed";
    }

    @GetMapping("/index")
    public String toIndex(){
        return "index";
    }
}
