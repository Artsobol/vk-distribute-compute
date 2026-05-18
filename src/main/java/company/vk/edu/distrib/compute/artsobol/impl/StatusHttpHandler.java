package company.vk.edu.distrib.compute.artsobol.impl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

final class StatusHttpHandler implements HttpHandler {
    private static final String METHOD_GET = "GET";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!METHOD_GET.equals(exchange.getRequestMethod())) {
            HttpResponses.writeEmpty(exchange, 405);
            return;
        }
        HttpResponses.writeEmpty(exchange, 200);
    }
}
