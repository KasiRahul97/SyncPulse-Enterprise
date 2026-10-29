package com.syncpulse.api;

import com.syncpulse.api.dto.ConflictResponse;
import com.syncpulse.persistence.ConflictRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/conflicts")
@Tag(name = "Conflicts", description = "Conflict audit trail")
public class ConflictController {

    private final ConflictRepository conflicts;

    public ConflictController(ConflictRepository conflicts) {
        this.conflicts = conflicts;
    }

    @GetMapping("/{userId}")
    @Operation(summary = "Most-recent conflict decisions for a user (newest first)")
    public List<ConflictResponse> forUser(@PathVariable String userId,
                                          @RequestParam(defaultValue = "50") int limit) {
        return conflicts.recentForUser(userId, Math.min(Math.max(limit, 1), 500))
                .stream().map(ConflictResponse::from).toList();
    }
}
