package company.vk.edu.distrib.compute.artsobol.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

record ErrorHttpHandler(HttpHandler delegate) implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(ErrorHttpHandler.class);

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException e) {
                HttpResponses.writeEmpty(exchange, 400);
            } catch (NoSuchElementException e) {
                HttpResponses.writeEmpty(exchange, 404);
            } catch (IOException e) {
                HttpResponses.writeEmpty(exchange, 500);
            } catch (RuntimeException e) {
                if (log.isErrorEnabled()) {
                    log.error("Unexpected exception while handling request", e);
                }
                HttpResponses.writeEmpty(exchange, 500);
            }
        }
    }
}
