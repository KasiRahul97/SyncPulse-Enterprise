package com.syncpulse.service;

/**
 * A single entry in the live activity feed shown on the dashboard.
 * {@code seq} is a monotonic id so the frontend can poll "everything since N".
 */
public record LiveEvent(
        long seq,
        long timestamp,
        String category,   // PING | DEBOUNCE | DECISION | EVENT | SYSTEM
        String userId,
        String contentId,
        String deviceId,
        String decision,   // nullable
        String comparison, // nullable
        Long positionSeconds,
        String message
) {}
