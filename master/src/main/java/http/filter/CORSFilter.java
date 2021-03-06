package http.filter;

import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class CORSFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        ((HttpServletResponse)response).addHeader("Access-Control-Allow-Methods", "*");
        //((HttpServletResponse)response).addHeader("Access-Control-Allow-Credentials", "true");
        //((HttpServletResponse)response).addHeader("Access-Control-Expose-Headers", "");

        String requestDomain = ((HttpServletRequest)request).getHeader("Origin");
        ((HttpServletResponse)response).addHeader("Access-Control-Allow-Origin", requestDomain);

        ((HttpServletResponse)response).addHeader("Access-Control-Allow-Headers", "Content-Type, Accept");
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {}
}
