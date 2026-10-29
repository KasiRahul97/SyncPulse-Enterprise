package com.syncpulse.common;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Turns exceptions into structured JSON errors; never swallows anything silently. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleBeanValidation(MethodArgumentNotValidException ex) {
        String msg = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("validation failed");
        return build(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", msg);
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(ValidationException ex) {
        return build(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", ex.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArg(IllegalArgumentException ex) {
        return build(HttpStatus.BAD_REQUEST, "BAD_REQUEST", ex.getMessage());
    }

    @ExceptionHandler({AllNodesFailedException.class, DriverException.class})
    public ResponseEntity<Map<String, Object>> handleCassandra(RuntimeException ex) {
        log.error("Cassandra unavailable: {}", ex.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "CASSANDRA_UNAVAILABLE",
                "The durable store is currently unavailable. Please retry.");
    }

    @ExceptionHandler(RedisConnectionFailureException.class)
    public ResponseEntity<Map<String, Object>> handleRedis(RedisConnectionFailureException ex) {
        log.error("Redis unavailable: {}", ex.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "REDIS_UNAVAILABLE",
                "The debounce layer is currently unavailable. Please retry.");
    }

    @ExceptionHandler(KafkaException.class)
    public ResponseEntity<Map<String, Object>> handleKafka(KafkaException ex) {
        log.error("Kafka error: {}", ex.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "KAFKA_UNAVAILABLE",
                "The event streaming layer is currently unavailable.");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception ex) {
        log.error("Unhandled error", ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                ex.getMessage() == null ? "Unexpected error" : ex.getMessage());
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, String code, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", code);
        body.put("message", message);
        body.put("requestId", MDC.get(RequestIdFilter.MDC_KEY));
        return ResponseEntity.status(status).body(body);
    }
}
