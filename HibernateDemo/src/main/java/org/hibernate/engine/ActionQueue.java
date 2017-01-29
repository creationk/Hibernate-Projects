package org.hibernate.engine;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.BulkOperationCleanupAction;
import org.hibernate.action.CollectionRecreateAction;
import org.hibernate.action.CollectionRemoveAction;
import org.hibernate.action.CollectionUpdateAction;
import org.hibernate.action.EntityDeleteAction;
import org.hibernate.action.EntityIdentityInsertAction;
import org.hibernate.action.EntityInsertAction;
import org.hibernate.action.EntityUpdateAction;
import org.hibernate.action.Executable;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.UpdateTimestampsCache;
import org.hibernate.cfg.Settings;
import org.hibernate.jdbc.Batcher;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActionQueue
{
  private static final Logger log = LoggerFactory.getLogger(ActionQueue.class);
  private static final int INIT_QUEUE_LIST_SIZE = 5;
  private SessionImplementor session;
  private ArrayList insertions;
  private ArrayList deletions;
  private ArrayList updates;
  private ArrayList collectionCreations;
  private ArrayList collectionUpdates;
  private ArrayList collectionRemovals;
  private ArrayList executions;
  
  public ActionQueue(SessionImplementor session)
  {
    this.session = session;
    init();
  }
  
  private void init()
  {
    this.insertions = new ArrayList(5);
    this.deletions = new ArrayList(5);
    this.updates = new ArrayList(5);
    
    this.collectionCreations = new ArrayList(5);
    this.collectionRemovals = new ArrayList(5);
    this.collectionUpdates = new ArrayList(5);
    
    this.executions = new ArrayList(15);
  }
  
  public void clear()
  {
    this.updates.clear();
    this.insertions.clear();
    this.deletions.clear();
    
    this.collectionCreations.clear();
    this.collectionRemovals.clear();
    this.collectionUpdates.clear();
  }
  
  public void addAction(EntityInsertAction action)
  {
    this.insertions.add(action);
  }
  
  public void addAction(EntityDeleteAction action)
  {
    this.deletions.add(action);
  }
  
  public void addAction(EntityUpdateAction action)
  {
    this.updates.add(action);
  }
  
  public void addAction(CollectionRecreateAction action)
  {
    this.collectionCreations.add(action);
  }
  
  public void addAction(CollectionRemoveAction action)
  {
    this.collectionRemovals.add(action);
  }
  
  public void addAction(CollectionUpdateAction action)
  {
    this.collectionUpdates.add(action);
  }
  
  public void addAction(EntityIdentityInsertAction insert)
  {
    this.insertions.add(insert);
  }
  
  public void addAction(BulkOperationCleanupAction cleanupAction)
  {
    this.executions.add(cleanupAction);
  }
  
  public void executeInserts()
    throws HibernateException
  {
    executeActions(this.insertions);
  }
  
  public void executeActions()
    throws HibernateException
  {
    executeActions(this.insertions);
    executeActions(this.updates);
    executeActions(this.collectionRemovals);
    executeActions(this.collectionUpdates);
    executeActions(this.collectionCreations);
    executeActions(this.deletions);
  }
  
  public void prepareActions()
    throws HibernateException
  {
    prepareActions(this.collectionRemovals);
    prepareActions(this.collectionUpdates);
    prepareActions(this.collectionCreations);
  }
  
  public void afterTransactionCompletion(boolean success)
  {
    int size = this.executions.size();
    boolean invalidateQueryCache = this.session.getFactory().getSettings().isQueryCacheEnabled();
    for (int i = 0; i < size; i++) {
      try
      {
        Executable exec = (Executable)this.executions.get(i);
        try
        {
          exec.afterTransactionCompletion(success);
        }
        finally
        {
          if (invalidateQueryCache) {
            this.session.getFactory().getUpdateTimestampsCache().invalidate(exec.getPropertySpaces());
          }
        }
      }
      catch (CacheException ce)
      {
        log.error("could not release a cache lock", ce);
      }
      catch (Exception e)
      {
        throw new AssertionFailure("Exception releasing cache locks", e);
      }
    }
    this.executions.clear();
  }
  
  public boolean areTablesToBeUpdated(Set tables)
  {
    return (areTablesToUpdated(this.updates, tables)) || (areTablesToUpdated(this.insertions, tables)) || (areTablesToUpdated(this.deletions, tables)) || (areTablesToUpdated(this.collectionUpdates, tables)) || (areTablesToUpdated(this.collectionCreations, tables)) || (areTablesToUpdated(this.collectionRemovals, tables));
  }
  
  public boolean areInsertionsOrDeletionsQueued()
  {
    return (this.insertions.size() > 0) || (this.deletions.size() > 0);
  }
  
  private static boolean areTablesToUpdated(List executables, Set tablespaces)
  {
    int size = executables.size();
    for (int j = 0; j < size; j++)
    {
      Serializable[] spaces = ((Executable)executables.get(j)).getPropertySpaces();
      for (int i = 0; i < spaces.length; i++) {
        if (tablespaces.contains(spaces[i]))
        {
          if (log.isDebugEnabled()) {
            log.debug("changes must be flushed to space: " + spaces[i]);
          }
          return true;
        }
      }
    }
    return false;
  }
  
  private void executeActions(List list)
    throws HibernateException
  {
    int size = list.size();
    for (int i = 0; i < size; i++) {
      execute((Executable)list.get(i));
    }
    list.clear();
    this.session.getBatcher().executeBatch();
  }
  
  public void execute(Executable executable)
  {
    boolean lockQueryCache = this.session.getFactory().getSettings().isQueryCacheEnabled();
    if ((executable.hasAfterTransactionCompletion()) || (lockQueryCache)) {
      this.executions.add(executable);
    }
    if (lockQueryCache) {
      this.session.getFactory().getUpdateTimestampsCache().preinvalidate(executable.getPropertySpaces());
    }
    executable.execute();
  }
  
  private void prepareActions(List queue)
    throws HibernateException
  {
    int size = queue.size();
    for (int i = 0; i < size; i++)
    {
      Executable executable = (Executable)queue.get(i);
      executable.beforeExecutions();
    }
  }
  
  public String toString()
  {
    return "ActionQueue[insertions=" + this.insertions + " updates=" + this.updates + " deletions=" + this.deletions + " collectionCreations=" + this.collectionCreations + " collectionRemovals=" + this.collectionRemovals + " collectionUpdates=" + this.collectionUpdates + "]";
  }
  
  public int numberOfCollectionRemovals()
  {
    return this.collectionRemovals.size();
  }
  
  public int numberOfCollectionUpdates()
  {
    return this.collectionUpdates.size();
  }
  
  public int numberOfCollectionCreations()
  {
    return this.collectionCreations.size();
  }
  
  public int numberOfDeletions()
  {
    return this.deletions.size();
  }
  
  public int numberOfUpdates()
  {
    return this.updates.size();
  }
  
  public int numberOfInsertions()
  {
    return this.insertions.size();
  }
  
  public void sortCollectionActions()
  {
    if (this.session.getFactory().getSettings().isOrderUpdatesEnabled())
    {
      Collections.sort(this.collectionCreations);
      Collections.sort(this.collectionUpdates);
      Collections.sort(this.collectionRemovals);
    }
  }
  
  public void sortActions()
  {
    if (this.session.getFactory().getSettings().isOrderUpdatesEnabled()) {
      Collections.sort(this.updates);
    }
    if (this.session.getFactory().getSettings().isOrderInsertsEnabled()) {
      sortInsertActions();
    }
  }
  
  private void sortInsertActions()
  {
    new InsertActionSorter().sort();
  }
  
  public ArrayList cloneDeletions()
  {
    return (ArrayList)this.deletions.clone();
  }
  
  public void clearFromFlushNeededCheck(int previousCollectionRemovalSize)
  {
    this.collectionCreations.clear();
    this.collectionUpdates.clear();
    this.updates.clear();
    for (int i = this.collectionRemovals.size() - 1; i >= previousCollectionRemovalSize; i--) {
      this.collectionRemovals.remove(i);
    }
  }
  
  public boolean hasAfterTransactionActions()
  {
    return this.executions.size() > 0;
  }
  
  public boolean hasAnyQueuedActions()
  {
    return (this.updates.size() > 0) || (this.insertions.size() > 0) || (this.deletions.size() > 0) || (this.collectionUpdates.size() > 0) || (this.collectionRemovals.size() > 0) || (this.collectionCreations.size() > 0);
  }
  
  public void serialize(ObjectOutputStream oos)
    throws IOException
  {
    log.trace("serializing action-queue");
    
    int queueSize = this.insertions.size();
    log.trace("starting serialization of [" + queueSize + "] insertions entries");
    oos.writeInt(queueSize);
    for (int i = 0; i < queueSize; i++) {
      oos.writeObject(this.insertions.get(i));
    }
    queueSize = this.deletions.size();
    log.trace("starting serialization of [" + queueSize + "] deletions entries");
    oos.writeInt(queueSize);
    for (int i = 0; i < queueSize; i++) {
      oos.writeObject(this.deletions.get(i));
    }
    queueSize = this.updates.size();
    log.trace("starting serialization of [" + queueSize + "] updates entries");
    oos.writeInt(queueSize);
    for (int i = 0; i < queueSize; i++) {
      oos.writeObject(this.updates.get(i));
    }
    queueSize = this.collectionUpdates.size();
    log.trace("starting serialization of [" + queueSize + "] collectionUpdates entries");
    oos.writeInt(queueSize);
    for (int i = 0; i < queueSize; i++) {
      oos.writeObject(this.collectionUpdates.get(i));
    }
    queueSize = this.collectionRemovals.size();
    log.trace("starting serialization of [" + queueSize + "] collectionRemovals entries");
    oos.writeInt(queueSize);
    for (int i = 0; i < queueSize; i++) {
      oos.writeObject(this.collectionRemovals.get(i));
    }
    queueSize = this.collectionCreations.size();
    log.trace("starting serialization of [" + queueSize + "] collectionCreations entries");
    oos.writeInt(queueSize);
    for (int i = 0; i < queueSize; i++) {
      oos.writeObject(this.collectionCreations.get(i));
    }
  }
  
  public static ActionQueue deserialize(ObjectInputStream ois, SessionImplementor session)
    throws IOException, ClassNotFoundException
  {
    log.trace("deserializing action-queue");
    ActionQueue rtn = new ActionQueue(session);
    
    int queueSize = ois.readInt();
    log.trace("starting deserialization of [" + queueSize + "] insertions entries");
    rtn.insertions = new ArrayList(queueSize);
    for (int i = 0; i < queueSize; i++) {
      rtn.insertions.add(ois.readObject());
    }
    queueSize = ois.readInt();
    log.trace("starting deserialization of [" + queueSize + "] deletions entries");
    rtn.deletions = new ArrayList(queueSize);
    for (int i = 0; i < queueSize; i++) {
      rtn.deletions.add(ois.readObject());
    }
    queueSize = ois.readInt();
    log.trace("starting deserialization of [" + queueSize + "] updates entries");
    rtn.updates = new ArrayList(queueSize);
    for (int i = 0; i < queueSize; i++) {
      rtn.updates.add(ois.readObject());
    }
    queueSize = ois.readInt();
    log.trace("starting deserialization of [" + queueSize + "] collectionUpdates entries");
    rtn.collectionUpdates = new ArrayList(queueSize);
    for (int i = 0; i < queueSize; i++) {
      rtn.collectionUpdates.add(ois.readObject());
    }
    queueSize = ois.readInt();
    log.trace("starting deserialization of [" + queueSize + "] collectionRemovals entries");
    rtn.collectionRemovals = new ArrayList(queueSize);
    for (int i = 0; i < queueSize; i++) {
      rtn.collectionRemovals.add(ois.readObject());
    }
    queueSize = ois.readInt();
    log.trace("starting deserialization of [" + queueSize + "] collectionCreations entries");
    rtn.collectionCreations = new ArrayList(queueSize);
    for (int i = 0; i < queueSize; i++) {
      rtn.collectionCreations.add(ois.readObject());
    }
    return rtn;
  }
  
  private class InsertActionSorter
  {
    private HashMap latestBatches = new HashMap();
    private HashMap entityBatchNumber;
    private HashMap actionBatches = new HashMap();
    
    public InsertActionSorter()
    {
      this.entityBatchNumber = new HashMap(ActionQueue.this.insertions.size() + 1, 1.0F);
    }
    
    public void sort()
    {
      for (Iterator actionItr = ActionQueue.this.insertions.iterator(); actionItr.hasNext();)
      {
        EntityInsertAction action = (EntityInsertAction)actionItr.next();
        
        String entityName = action.getEntityName();
        
        Object currentEntity = action.getInstance();
        Integer batchNumber;
        if (this.latestBatches.containsKey(entityName))
        {
          batchNumber = findBatchNumber(action, entityName);
        }
        else
        {
          batchNumber = new Integer(this.actionBatches.size());
          this.latestBatches.put(entityName, batchNumber);
        }
        this.entityBatchNumber.put(currentEntity, batchNumber);
        addToBatch(batchNumber, action);
      }
      ActionQueue.this.insertions.clear();
      Iterator batchItr;
      for (int i = 0; i < this.actionBatches.size(); i++)
      {
        List batch = (List)this.actionBatches.get(new Integer(i));
        for (batchItr = batch.iterator(); batchItr.hasNext();)
        {
          EntityInsertAction action = (EntityInsertAction)batchItr.next();
          ActionQueue.this.insertions.add(action);
        }
      }
    }
    
    private Integer findBatchNumber(EntityInsertAction action, String entityName)
    {
      Integer latestBatchNumberForType = (Integer)this.latestBatches.get(entityName);
      
      Object[] propertyValues = action.getState();
      Type[] propertyTypes = action.getPersister().getClassMetadata().getPropertyTypes();
      for (int i = 0; i < propertyValues.length; i++)
      {
        Object value = propertyValues[i];
        Type type = propertyTypes[i];
        if ((type.isEntityType()) && (value != null))
        {
          Integer associationBatchNumber = (Integer)this.entityBatchNumber.get(value);
          if ((associationBatchNumber != null) && (associationBatchNumber.compareTo(latestBatchNumberForType) > 0))
          {
            latestBatchNumberForType = new Integer(this.actionBatches.size());
            this.latestBatches.put(entityName, latestBatchNumberForType);
            
            break;
          }
        }
      }
      return latestBatchNumberForType;
    }
    
    private void addToBatch(Integer batchNumber, EntityInsertAction action)
    {
      List actions = (List)this.actionBatches.get(batchNumber);
      if (actions == null)
      {
        actions = new LinkedList();
        this.actionBatches.put(batchNumber, actions);
      }
      actions.add(action);
    }
  }
}
