package org.jobgovernance.core.model;

public sealed interface ConcurrencyPolicy permits
        ConcurrencyPolicy.ForbidOverlap,
        ConcurrencyPolicy.AllowOverlap,
        ConcurrencyPolicy.AllowOverlapUpTo,
        ConcurrencyPolicy.SingletonClusterWide,
        ConcurrencyPolicy.SingletonPerTenant,
        ConcurrencyPolicy.ShardByPartitionKey {

    Kind kind();

    enum Kind {
        FORBID_OVERLAP,
        ALLOW_OVERLAP,
        ALLOW_OVERLAP_UP_TO,
        SINGLETON_CLUSTER_WIDE,
        SINGLETON_PER_TENANT,
        SHARD_BY_PARTITION_KEY
    }

    record ForbidOverlap() implements ConcurrencyPolicy {
        @Override
        public Kind kind() {
            return Kind.FORBID_OVERLAP;
        }
    }

    record AllowOverlap() implements ConcurrencyPolicy {
        @Override
        public Kind kind() {
            return Kind.ALLOW_OVERLAP;
        }
    }

    record AllowOverlapUpTo(int maxParallelExecutions) implements ConcurrencyPolicy {
        public AllowOverlapUpTo {
            if (maxParallelExecutions < 1) {
                throw new IllegalArgumentException("maxParallelExecutions must be >= 1");
            }
        }

        @Override
        public Kind kind() {
            return Kind.ALLOW_OVERLAP_UP_TO;
        }
    }

    record SingletonClusterWide() implements ConcurrencyPolicy {
        @Override
        public Kind kind() {
            return Kind.SINGLETON_CLUSTER_WIDE;
        }
    }

    record SingletonPerTenant() implements ConcurrencyPolicy {
        @Override
        public Kind kind() {
            return Kind.SINGLETON_PER_TENANT;
        }
    }

    record ShardByPartitionKey(int maxParallelPerShard) implements ConcurrencyPolicy {
        public ShardByPartitionKey {
            if (maxParallelPerShard < 1) {
                throw new IllegalArgumentException("maxParallelPerShard must be >= 1");
            }
        }

        @Override
        public Kind kind() {
            return Kind.SHARD_BY_PARTITION_KEY;
        }
    }
}
