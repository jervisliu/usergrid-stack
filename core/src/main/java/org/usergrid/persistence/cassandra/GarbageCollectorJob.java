package org.usergrid.persistence.cassandra;

import me.prettyprint.hector.api.mutation.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.EntityRef;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.usergrid.persistence.Schema.getDefaultSchema;
import static org.usergrid.persistence.cassandra.ApplicationCF.ENTITY_COMPOSITE_DICTIONARIES;
import static org.usergrid.persistence.cassandra.ApplicationCF.ENTITY_DICTIONARIES;
import static org.usergrid.persistence.cassandra.CassandraPersistenceUtils.addDeleteToMutator;
import static org.usergrid.persistence.cassandra.CassandraPersistenceUtils.batchExecute;
import static org.usergrid.persistence.cassandra.CassandraPersistenceUtils.key;
import static org.usergrid.utils.UUIDUtils.getTimestampInMicros;

class GarbageCollectorJob implements Callable {

  private static final Logger logger = LoggerFactory.getLogger(GarbageCollectorJob.class);

  private EntityManagerImpl em;
  private Mutator<ByteBuffer> m;
  private EntityRef entity;
  private UUID timestampUuid;

  public GarbageCollectorJob(EntityManagerImpl em,
                             Mutator<ByteBuffer> m,
                             EntityRef entity,
                             UUID timestampUuid) throws Exception {
    this.em = em;
    this.m = m;
    this.entity = entity;
    this.timestampUuid = timestampUuid;
  }

  public Object call() throws Exception {
    // disconnect all connections
    RelationManagerImpl relationManager = em.getRelationManager(entity);
    relationManager.batchDisconnect(m, timestampUuid);

    // delete all core properties and any dynamic property that's ever been
    // in the dictionary for this entity
    Set<String> properties = em.getPropertyNames(entity);
    properties.remove("uuid"); // already deleted
    if (properties != null) {
      for (String propertyName : properties) {
        em.batchSetProperty(m, entity, propertyName, null, true, false, timestampUuid);
      }
    }

    // delete any attributes in core dictionaries and dynamic dictionaries
    // associated with this entity
    Set<String> dictionaries = em.getDictionaryNames(entity);
    if (dictionaries != null) {
      for (String dictionary : dictionaries) {
        Set<Object> values = em.getDictionaryAsSet(entity, dictionary);
        if (values != null) {
          for (Object value : values) {
            em.batchUpdateDictionary(m, entity, dictionary, value, true, timestampUuid);
          }
        }
      }
    }

    // remove from all containing collections
    relationManager.batchRemoveFromContainers(m, timestampUuid);

    // delete entity UUID from dictionaries
    if (dictionaries != null) {
      long timestamp = getTimestampInMicros(timestampUuid) + 1;
      for (String dictionary : dictionaries) {
        ApplicationCF cf = getDefaultSchema().hasDictionary(entity.getType(), dictionary)
            ? ENTITY_DICTIONARIES
            : ENTITY_COMPOSITE_DICTIONARIES;

        addDeleteToMutator(m, cf, key(entity.getUuid(), dictionary), timestamp);
      }
    }
    // todo: remove later
    logger.error("batchExecute Mutator with {} mutations", m.getPendingMutationCount());
    batchExecute(m, CassandraService.RETRY_COUNT);
    return null;
  }
}

