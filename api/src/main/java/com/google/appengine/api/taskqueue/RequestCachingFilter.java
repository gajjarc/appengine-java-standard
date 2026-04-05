package com.google.appengine.api.taskqueue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class RequestCachingFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        System.out.println("RequestCachingFilter: Initialized");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            
            // We only need to cache for POST requests which might have payloads
            if ("POST".equalsIgnoreCase(httpRequest.getMethod())) {
                String uri = httpRequest.getRequestURI();
                // Only cache for task handler or sweep endpoints to avoid overhead on other requests
                if (uri.contains("/task-handler") || uri.contains("/_ah/cloudtask/sweep")) {
                    System.out.println("RequestCachingFilter: Caching request for URI: " + uri);
                    CachedRequestWrapper wrappedRequest = new CachedRequestWrapper(httpRequest);
                    chain.doFilter(wrappedRequest, response);
                    return;
                }
            }
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {}

    private static class CachedRequestWrapper extends HttpServletRequestWrapper {
        private final byte[] cachedBody;

        public CachedRequestWrapper(HttpServletRequest request) throws IOException {
            super(request);
            java.io.InputStream is = request.getInputStream();
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            int len;
            while ((len = is.read(buf)) > 0) {
                baos.write(buf, 0, len);
            }
            this.cachedBody = baos.toByteArray();
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            final ByteArrayInputStream bais = new ByteArrayInputStream(cachedBody);
            return new ServletInputStream() {
                @Override
                public boolean isFinished() {
                    return bais.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int read() throws IOException {
                    return bais.read();
                }
            };
        }

        @Override
        public BufferedReader getReader() throws IOException {
            String charset = getCharacterEncoding();
            if (charset == null) charset = "UTF-8";
            return new BufferedReader(new InputStreamReader(getInputStream(), charset));
        }
    }
}
