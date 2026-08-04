package com.google.appengine.api.taskqueue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;
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

/**
 * Servlet filter that caches the input stream of HTTP POST requests directed to task handlers or
 * the sweeper endpoint.
 *
 * <p>This caching ensures that task payloads can be read reliably across multiple filter or servlet
 * invocations without exhausting the underlying request input stream.
 */
public class RequestCachingFilter implements Filter {
    private static final Logger logger = Logger.getLogger(RequestCachingFilter.class.getName());

    /**
     * Initializes the filter with the specified configuration.
     *
     * @param filterConfig the filter configuration object
     * @throws ServletException if initialization fails
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("RequestCachingFilter: Initialized");
    }

    /**
     * Inspects incoming HTTP POST requests and wraps matching task request streams with a
     * {@link CachedRequestWrapper} to allow repeated stream reads.
     *
     * @param request the incoming servlet request
     * @param response the outgoing servlet response
     * @param chain the filter chain for processing the request
     * @throws IOException if an I/O error occurs during request processing
     * @throws ServletException if a servlet error occurs during request processing
     */
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
                    logger.info("RequestCachingFilter: Caching request for URI: " + uri);
                    CachedRequestWrapper wrappedRequest = new CachedRequestWrapper(httpRequest);
                    chain.doFilter(wrappedRequest, response);
                    return;
                }
            }
        }
        chain.doFilter(request, response);
    }

    /**
     * Cleans up resources held by this filter upon destruction by the servlet container.
     */
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
