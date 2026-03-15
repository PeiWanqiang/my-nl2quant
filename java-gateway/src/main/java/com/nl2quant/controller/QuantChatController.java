package com.nl2quant.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/gateway")
@CrossOrigin(origins = "*") // Allow frontend requests for MVP
public class QuantChatController {

    private final WebClient webClient;

    public QuantChatController(WebClient.Builder webClientBuilder, @Value("${nl2quant.python-engine.url}") String engineUrl) {
        this.webClient = webClientBuilder.baseUrl(engineUrl).build();
    }

    @PostMapping("/chat/negotiate")
    public Mono<ResponseEntity<Map>> negotiate(@RequestBody Map<String, Object> requestBody) {
        // Forwarding directly to Python Engine for MVP.
        // In reality, Java would track session, append message history, etc.
        return webClient.post()
                .uri("/api/v1/chat/negotiate")
                .bodyValue(requestBody)
                .retrieve()
                .toEntity(Map.class);
    }
    
    @PostMapping("/quant/execute")
    public Mono<ResponseEntity<Map>> execute(@RequestBody Map<String, Object> requestBody) {
        return webClient.post()
                .uri("/api/v1/quant/execute")
                .bodyValue(requestBody)
                .retrieve()
                .toEntity(Map.class);
    }
}
