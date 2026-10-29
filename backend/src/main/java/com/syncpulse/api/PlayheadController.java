package com.syncpulse.api;

import com.syncpulse.api.dto.PlayheadStateResponse;
import com.syncpulse.api.dto.PlayheadUpdateRequest;
import com.syncpulse.api.dto.PlayheadUpdateResponse;
import com.syncpulse.service.PlayheadService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/playhead")
@Tag(name = "Playhead", description = "Ingest playhead updates and read authoritative state")
public class PlayheadController {

    private final PlayheadService service;

    public PlayheadController(PlayheadService service) {
        this.service = service;
    }

    @PostMapping("/update")
    @Operation(summary = "Submit a playhead update through the full pipeline " +
            "(debounce → conflict engine → Cassandra → CDC/Kafka)")
    public PlayheadUpdateResponse update(@Valid @RequestBody PlayheadUpdateRequest request) {
        return service.update(request);
    }

    @GetMapping("/{userId}/{contentId}")
    @Operation(summary = "Read the authoritative playhead state for a user/content pair")
    public ResponseEntity<PlayheadStateResponse> get(@PathVariable String userId,
                                                     @PathVariable String contentId) {
        return service.getState(userId, contentId)
                .map(PlayheadStateResponse::from)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
