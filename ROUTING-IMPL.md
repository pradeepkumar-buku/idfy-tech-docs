### 🏛️ High-Level Component Architecture

    ┌────────────────────────────────────────────────────────────────────────────────────────┐
    │ 1. API Layer (App Module)                                                              │
    │    • KycInitiateController   ➜ POST /janus/api/kyc/initiate                            │
    │    • OpsRoutingController    ➜ GET/POST /janus/ops/routing/config & /refresh          │
    ├────────────────────────────────────────────────────────────────────────────────────────┤
    │ 2. Core Domain & Business Logic (Core Module)                                          │
    │    • KycRoutingService       ➜ Implements 5-step routing algorithm & fallback          │
    │    • RoutingRefreshService   ➜ Rebuilds 100-slot rank array in Redis                   │
    │    • Output Ports:                                                                     │
    │      - LoadDistributorPort   ➜ Redis operation contract (RList, RAtomicLong)           │
    │      - LoadDistributorDbPort ➜ Database query/update contract                        │
    ├────────────────────────────────────────────────────────────────────────────────────────┤
    │ 3. Persistence & Infrastructure Layer (Persistence Module)                             │
    │    • LoadDistributorRedisAdapter ➜ Redisson atomic batch INCR & 100-slot RList          │
    │    • LoadDistributorDbAdapter    ➜ PostgreSQL Spring Data Repositories                 │
    │    • Database Tables (Flyway Migrations):                                             │
    │      - load_distribution_config ➜ Config weights & rank assignments per app_origin     │
    │      - load_distributor         ➜ 100-slot rank JSONB array per app_origin            │
    │      - user_provider_routing    ➜ User journey tracking & sticky routing state        │
    └────────────────────────────────────────────────────────────────────────────────────────┘
    ──────
  ### 📦 Codebase File References

  #### 1. REST Controllers (app module)

  • **KycInitiateController.java**
      • Unified initiation entrypoint POST /janus/api/kyc/initiate.
      • Reads Boku-origin request header (e.g. bukuwarung-app), delegates to KycRoutingService.selectProvider(), and returns either NATIVE (VIDA) or HOSTED (IDfy
      redirect URL).
  • **OpsRoutingController.java**
      • Operational APIs for Ops to view current provider allocations, update percentage weights, and trigger Redis distributor reloads.


  #### 2. Core Business Logic (core module)

  • **KycRoutingService.java**
      • Implements the routing decision tree:
          1. Check for in-flight IN_PROGRESS journey (idempotency).
          2. Check for completed/verified prior attempt (sticky routing).
          3. Perform atomic Redis counter increment → slot index calculation → rank resolution → DB provider lookup.
          4. Deterministic hash fallback if Redis is unavailable (abs(accountId.hashCode()) % 100).
          5. Record journey in user_provider_routing.

  • **RoutingRefreshService.java**
      • Generates 100 rank integer slots per app_origin based on configured percentage allocations, shuffles them via Fisher-Yates, and atomically reloads Redis via
      @PostConstruct or Ops trigger.
  • **LoadDistributorPort.java**
      • Output port defining Redis primitive contracts (getRankAtSlot, incrementCounter, rebuildDistributor).
  • **LoadDistributorDbPort.java**
      • Output port defining database persistence contracts (findActiveConfigsByApp, saveRoutingRow, updateRoutingStatus).


  #### 3. Persistence Adapters & Entities (persistence module)

  • **LoadDistributorRedisAdapter.java**
      • Implements Redisson Redis calls:
          • Key {prefix}:routing:distributor:{app_origin} → RList<Integer> (100 rank slots).
          • Key {prefix}:routing:counter:{app_origin} → RAtomicLong (atomic request counter).

  • **LoadDistributorDbAdapter.java**
      • Implements database read/write queries using Spring Data JPA Repositories.
  • Entities:
      • UserProviderRoutingEntity.java (user_provider_routing table).
      • LoadDistributionConfigEntity.java (load_distribution_config table).
      • LoadDistributorEntity.java (load_distributor table).

  ──────
  ### 🗄️ Database Schema & Migration References

  Flyway Migration File: **v20260410130000__create_dynamic_routing_tables.sql**

  #### 1. load_distribution_config

    CREATE TABLE load_distribution_config (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        app_origin VARCHAR(64) NOT NULL,
        provider VARCHAR(32) NOT NULL,
        allocation INT NOT NULL CHECK (allocation >= 0 AND allocation <= 100),
        rank INT NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_app_provider UNIQUE (app_origin, provider)
    );

  #### 2. load_distributor

    CREATE TABLE load_distributor (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        app_origin VARCHAR(64) NOT NULL UNIQUE,
        load_distributor JSONB NOT NULL, -- Array of 100 rank integers e.g. [1, 2, 1, 1, 2...]
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
    );

  #### 3. user_provider_routing

    CREATE TABLE user_provider_routing (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        account_id VARCHAR(64) NOT NULL,
        provider_used VARCHAR(32) NOT NULL,
        status VARCHAR(32) NOT NULL, -- 'IN_PROGRESS', 'VERIFIED', 'REJECTED', 'PENDING_MANUAL_VERIFICATION'
        app_used VARCHAR(64),
        metadata JSONB,              -- Stores e.g. {"resolved_rank": 1, "redis_slot_index": 42}
        created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
    );
