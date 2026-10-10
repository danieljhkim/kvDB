package com.danieljhkim.kvdb.kvadmin.api;

import com.danieljhkim.kvdb.kvadmin.api.dto.TriggerRequestDto;
import com.danieljhkim.kvdb.kvadmin.service.OpsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for operational tasks.
 *
 * <p>{@code POST /admin/ops/compact} and {@code COMPACT} on the generic trigger endpoint return 501 until
 * node compaction is implemented.
 */
@RestController
@RequestMapping("/admin/ops")
@RequiredArgsConstructor
public class OpsController {

    private final OpsService opsService;

    @PostMapping("/rebalance")
    public ResponseEntity<TriggerRequestDto> triggerRebalance(@RequestBody TriggerRequestDto request) {
        TriggerRequestDto result = opsService.triggerRebalance(request);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/compact")
    public ResponseEntity<TriggerRequestDto> triggerCompaction(@RequestBody TriggerRequestDto request) {
        TriggerRequestDto result = opsService.triggerCompaction();
        return ResponseEntity.ok(result);
    }

    @PostMapping("/trigger")
    public ResponseEntity<TriggerRequestDto> triggerOperation(
            @Validated(TriggerRequestDto.OnTrigger.class) @RequestBody TriggerRequestDto request) {
        TriggerRequestDto result = opsService.triggerOperation(request);
        return ResponseEntity.ok(result);
    }
}
