package com.we.dreams.thousand.interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * @author ming
 * @version V1.0
 * @Package com.we.dreams.thousand.interceptor
 * @date 2020/3/13 10:31
 * @description 登录拦截器
 */
public class LoginInterceptor implements HandlerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LoginInterceptor.class);

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //请求拦截
        LOG.info("=====登录状态拦截====");
        LOG.info("请求链接 => {}", request.getRequestURL().toString());

        //获取登录Session
        HttpSession session = request.getSession();
        LOG.info("session id => {}", session.getId());

        //验证登录
        Object userInfo = session.getAttribute("userInfo");
        if(userInfo == null){
            LOG.warn("没有登录");
//            response.getWriter().println("请先登录");
            response.sendRedirect("/login");
            return false;
        }else{
            LOG.info("已登录, 用户信息为 => {}", userInfo);
        }
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {

    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {

    }
}
