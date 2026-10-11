package com.danieljhkim.kvdb.kvadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.danieljhkim.kvdb.kvadmin.api.dto.ShardMapSnapshotDto;
import com.danieljhkim.kvdb.kvadmin.client.CoordinatorReadClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ClusterAdminServiceTest {

    @Test
    void shardMapVersionFailsWhenCoordinatorReturnsNoShardMap() {
        StubCoordinatorReadClient coordinator = new StubCoordinatorReadClient();
        ClusterAdminService service = newService(coordinator);

        assertThrows(IllegalStateException.class, service::getShardMapVersion);
    }

    @Test
    void shardMapVersionFailsWhenCoordinatorCallFails() {
        StubCoordinatorReadClient coordinator = new StubCoordinatorReadClient();
        coordinator.failWith(
                Status.UNAVAILABLE.withDescription("coordinator down").asRuntimeException());
        ClusterAdminService service = newService(coordinator);

        assertThrows(StatusRuntimeException.class, service::getShardMapVersion);
    }

    @Test
    void shardMapVersionReturnsZeroWhenCoordinatorReportsZero() {
        StubCoordinatorReadClient coordinator = new StubCoordinatorReadClient();
        coordinator.shardMap = snapshot(0);
        ClusterAdminService service = newService(coordinator);

        assertEquals(0, service.getShardMapVersion());
    }

    @Test
    void shardMapVersionReturnsZeroFromCacheWhenCachedVersionIsZero() {
        StubCoordinatorReadClient coordinator = new StubCoordinatorReadClient();
        coordinator.shardMap = snapshot(0);
        ClusterAdminService service = newService(coordinator);
        assertEquals(0, service.getShardMapVersion());

        // A cache miss would throw here, so a 0 result proves the cached zero was used
        coordinator.shardMap = null;
        assertEquals(0, service.getShardMapVersion());
    }

    private static ClusterAdminService newService(CoordinatorReadClient coordinator) {
        return new ClusterAdminService(coordinator, null, null, new ShardMapCache());
    }

    private static ShardMapSnapshotDto snapshot(long mapVersion) {
        return ShardMapSnapshotDto.builder().mapVersion(mapVersion).build();
    }

    private static final class StubCoordinatorReadClient extends CoordinatorReadClient {
        private ShardMapSnapshotDto shardMap;
        private StatusRuntimeException failure;

        StubCoordinatorReadClient() {
            super(List.of("localhost:1"), 1, TimeUnit.MILLISECONDS);
        }

        void failWith(StatusRuntimeException error) {
            this.failure = error;
        }

        @Override
        public ShardMapSnapshotDto getShardMap() {
            if (failure != null) {
                throw failure;
            }
            return shardMap;
        }
    }
}
