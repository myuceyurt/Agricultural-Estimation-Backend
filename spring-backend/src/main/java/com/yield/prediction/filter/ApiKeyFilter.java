package com.yield.prediction.filter;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import java.io.IOException;

@Component
@Order(1)
public class ApiKeyFilter implements Filter {

    @Value("${app.api.key}")
    private String secretKey;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;

        String clientKey = req.getHeader("X-API-KEY");

        if (secretKey.equals(clientKey)) {
            chain.doFilter(request, response);
        } else {
            res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            res.getWriter().write("Unauthorized: Invalid API Key");
        }
    }
}