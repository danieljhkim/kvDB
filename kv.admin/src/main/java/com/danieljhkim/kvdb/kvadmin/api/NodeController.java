package com.danieljhkim.kvdb.kvadmin.api;

import com.danieljhkim.kvdb.kvadmin.api.dto.HealthDto;
import com.danieljhkim.kvdb.kvadmin.api.dto.NodeDto;
import com.danieljhkim.kvdb.kvadmin.service.NodeAdminService;
import java.util.List;
import lombok.RequiredArgsConstructor;
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
 * /admin/nodes/{nodeId}/status - Update node status
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

    @PostMapping("/{nodeId}/status")
    public ResponseEntity<NodeDto> setNodeStatus(@PathVariable("nodeId") String nodeId, @RequestBody String status) {
        NodeDto node = nodeAdminService.setNodeStatus(nodeId, status);
        return ResponseEntity.ok(node);
    }
}
