package com.we.dreams.thousand.controller;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.we.dreams.thousand.dto.LoginDto;
import com.we.dreams.thousand.dto.ResultDto;
import com.we.dreams.thousand.model.TbUser;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
    public ResultDto login(@RequestBody LoginDto paramLogin){

        ResultDto resultDto = new ResultDto();

        LOG.info("input username : {}", paramLogin.getUsername());
        LOG.info("input password : {}", paramLogin.getPassword());

        String username = paramLogin.getUsername();
        String password = paramLogin.getPassword();

        TbUser user = new TbUser();
        //省略MD5
        QueryWrapper<TbUser> wrapper = new QueryWrapper<>();
        wrapper.eq("user_name", username)
               .eq("password", password);
        TbUser tbUser = user.selectOne(wrapper);
        if(tbUser != null){
            LOG.info("db username => {}", tbUser.getUserName());
            LOG.info("db password => {}", tbUser.getPassword());
            resultDto.setModule(tbUser);
            resultDto.setSuccess(true);
        }
        return resultDto;
    }

    @GetMapping("/login")
    public String toLogin(){
        return "login";
    }

    @GetMapping("/index")
    public String toIndex(){
        return "index";
    }


}
