package com.danieljhkim.kvdb.kvadmin.api;

import com.danieljhkim.kvdb.kvadmin.api.dto.HealthDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.SetNodeStatusRequestDto;
import com.danieljhkim.kvdb.kvadmin.service.NodeAdminService;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for node operations.
 *
 * <p>
 * Endpoints: - GET /admin/nodes - List all nodes - GET /admin/nodes/{nodeId} - Get node details - GET
 * /admin/nodes/{nodeId}/health - Get node health - POST /admin/nodes - Register a new node - POST
 * /admin/nodes/{nodeId}/status - Update node status from JSON {@code {"status":"ALIVE|SUSPECT|DEAD"}}
 */
@RestController
@RequestMapping("/admin/nodes")
@RequiredArgsConstructor
public class NodeController {

    private final NodeAdminService nodeAdminService;

    @GetMapping
    public ResponseEntity<List<NodeDto>> listNodes() {
        List<NodeDto> nodes = nodeAdminService.listNodes();
        return ResponseEntity.ok(nodes);
    }

    @GetMapping("/{nodeId}")
    public ResponseEntity<NodeDto> getNode(@PathVariable("nodeId") String nodeId) {
        NodeDto node = nodeAdminService.getNode(nodeId);
        return ResponseEntity.ok(node);
    }

    @GetMapping("/{nodeId}/health")
    public ResponseEntity<HealthDto> getNodeHealth(@PathVariable("nodeId") String nodeId) {
        HealthDto health = nodeAdminService.getNodeHealth(nodeId);
        return ResponseEntity.ok(health);
    }

    @PostMapping
    public ResponseEntity<NodeDto> registerNode(@RequestBody NodeDto node) {
        NodeDto registered = nodeAdminService.registerNode(node);
        return ResponseEntity.ok(registered);
    }

    /**
     * Set a node's status. Requires {@code application/json}. {@code status} must be {@code ALIVE}, {@code SUSPECT},
     * or {@code DEAD}. The raw request body is never stored as the status.
     */
    @PostMapping(value = "/{nodeId}/status", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<NodeDto> setNodeStatus(
            @PathVariable("nodeId") String nodeId, @Valid @RequestBody SetNodeStatusRequestDto request) {
        NodeDto node = nodeAdminService.setNodeStatus(nodeId, request.getStatus());
        return ResponseEntity.ok(node);
    }
}
