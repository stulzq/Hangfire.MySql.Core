using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Dapper;

using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Oracle.Core.Entities;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Oracle.Core
{
    internal class OracleWriteOnlyTransaction : JobStorageTransaction
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(OracleWriteOnlyTransaction));

        private readonly OracleStorage _storage;

        private readonly Queue<Action<IDbConnection>> _commandQueue = new Queue<Action<IDbConnection>>();

        public OracleWriteOnlyTransaction(OracleStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            Logger.TraceFormat("ExpireJob jobId={0}", jobId);

            AcquireJobLock();

            QueueCommand(x =>
                x.Execute(
                    "UPDATE MISP.HF_JOB SET EXPIRE_AT = :EXPIRE_AT WHERE ID = :ID",
                    new { EXPIRE_AT = DateTime.UtcNow.Add(expireIn), ID = jobId }));
        }

        public override void PersistJob(string jobId)
        {
            Logger.TraceFormat("PersistJob jobId={0}", jobId);

            AcquireJobLock();

            QueueCommand(x => x.Execute("UPDATE MISP.HF_JOB SET EXPIRE_AT = NULL WHERE ID = :ID", new { ID = jobId }));
        }

        public override void SetJobState(string jobId, IState state)
        {
            Logger.TraceFormat("SetJobState jobId={0}", jobId);

            AcquireStateLock();
            AcquireJobLock();

            var stateId = _storage.UseConnection(connection => connection.GetNextId());
            QueueCommand(x => x.Execute(
                @"
BEGIN
 INSERT INTO MISP.HF_JOB_STATE (ID, JOB_ID, NAME, REASON, CREATED_AT, DATA)
      VALUES (:STATE_ID, :JOB_ID, :NAME, :REASON, :CREATED_AT, :DATA);
 
      UPDATE MISP.HF_JOB SET STATE_ID = :STATE_ID, STATE_NAME = :NAME WHERE ID = :ID;
END;
",
                new
                {
                    STATE_ID = stateId,
                    JOB_ID = jobId,
                    NAME = state.Name,
                    REASON = state.Reason,
                    CREATED_AT = DateTime.UtcNow,
                    DATA = JobHelper.ToJson(state.SerializeData()),
                    ID = jobId
                }));
        }

        public override void AddJobState(string jobId, IState state)
        {
            Logger.TraceFormat("AddJobState jobId={0}, state={1}", jobId, state);

            AcquireStateLock();

            QueueCommand(x => x.Execute(
                " INSERT INTO MISP.HF_JOB_STATE (ID, JOB_ID, NAME, REASON, CREATED_AT, DATA) " +
                "      VALUES (MISP.HF_SEQUENCE.NEXTVAL, :JOB_ID, :NAME, :REASON, :CREATED_AT, :DATA)",
                new
                {
                    JOB_ID = jobId,
                    NAME = state.Name,
                    REASON = state.Reason,
                    CREATED_AT = DateTime.UtcNow,
                    DATA = JobHelper.ToJson(state.SerializeData())
                }));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            Logger.TraceFormat("AddToQueue jobId={0}", jobId);

            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            QueueCommand(x => persistentQueue.Enqueue(x, queue, jobId));
        }

        public override void IncrementCounter(string key)
        {
            Logger.TraceFormat("IncrementCounter key={0}", key);

            AcquireCounterLock();

            QueueCommand(x => x.Execute("INSERT INTO MISP.HF_COUNTER (ID, KEY, VALUE) values (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE)",
                    new { KEY = key, VALUE = +1 }));
        }


        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("IncrementCounter key={0}, expireIn={1}", key, expireIn);

            AcquireCounterLock();

            QueueCommand(x =>
                x.Execute(
                    "INSERT INTO MISP.HF_COUNTER (ID, KEY, VALUE, EXPIRE_AT) values (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE, :EXPIRE_AT)",
                    new { KEY = key, VALUE = +1, EXPIRE_AT = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void DecrementCounter(string key)
        {
            Logger.TraceFormat("DecrementCounter key={0}", key);

            AcquireCounterLock();

            QueueCommand(x =>
                x.Execute(
                    "INSERT INTO MISP.HF_COUNTER (ID, KEY, VALUE) values (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE)",
                    new { KEY = key, VALUE = -1 }));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("DecrementCounter key={0} expireIn={1}", key, expireIn);

            AcquireCounterLock();
            QueueCommand(x =>
                x.Execute(
                    "INSERT INTO MISP.HF_COUNTER (ID, KEY, VALUE, EXPIRE_AT) values (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE, :EXPIRE_AT)",
                    new { KEY = key, VALUE = -1, EXPIRE_AT = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            Logger.TraceFormat("AddToSet key={0} value={1}", key, value);

            AcquireSetLock();

            QueueCommand(x => x.Execute(
                @"
 MERGE INTO MISP.HF_SET H
      USING (SELECT 1 FROM DUAL) SRC
         ON (H.KEY = :KEY AND H.VALUE = :VALUE)
 WHEN MATCHED THEN
      UPDATE SET SCORE = :SCORE
 WHEN NOT MATCHED THEN
      INSERT (ID, KEY, VALUE, SCORE)
      VALUES (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE, :SCORE)
",
                new { KEY = key, VALUE = value, SCORE = score }));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            Logger.TraceFormat("AddRangeToSet key={0}", key);

            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            AcquireSetLock();
            QueueCommand(x =>
                x.Execute(
                    "INSERT INTO MISP.HF_SET (ID, KEY, VALUE, SCORE) VALUES (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE, 0.0)",
                    items.Select(value => new { KEY = key, VALUE = value }).ToList()));
        }


        public override void RemoveFromSet(string key, string value)
        {
            Logger.TraceFormat("RemoveFromSet key={0} value={1}", key, value);

            AcquireSetLock();
            QueueCommand(x => x.Execute("DELETE FROM MISP.HF_SET WHERE KEY = :KEY AND VALUE = :VALUE", new { KEY = key, VALUE = value }));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("ExpireSet key={0} expirein={1}", key, expireIn);

            if (key == null) throw new ArgumentNullException(nameof(key));

            AcquireSetLock();
            QueueCommand(x =>
                x.Execute(
                    "UPDATE MISP.HF_SET SET EXPIRE_AT = :EXPIRE_AT WHERE KEY = :KEY",
                    new { KEY = key, EXPIRE_AT = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void InsertToList(string key, string value)
        {
            Logger.TraceFormat("InsertToList key={0} value={1}", key, value);

            AcquireListLock();
            QueueCommand(x => x.Execute(
                "INSERT INTO MISP.HF_LIST (ID, KEY, VALUE) VALUES (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE)",
                new { KEY = key, VALUE = value }));
        }


        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            Logger.TraceFormat("ExpireList key={0} expirein={1}", key, expireIn);

            AcquireListLock();
            QueueCommand(x =>
                x.Execute(
                    "UPDATE MISP.HF_LIST SET EXPIRE_AT = :EXPIRE_AT WHERE KEY = :KEY",
                    new { KEY = key, EXPIRE_AT = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void RemoveFromList(string key, string value)
        {
            Logger.TraceFormat("RemoveFromList key={0} value={1}", key, value);

            AcquireListLock();
            QueueCommand(x => x.Execute(
                "DELETE FROM MISP.HF_LIST WHERE KEY = :KEY AND VALUE = :VALUE",
                new { KEY = key, VALUE = value }));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            Logger.TraceFormat("TrimList key={0} from={1} to={2}", key, keepStartingFrom, keepEndingAt);

            AcquireListLock();
            QueueCommand(x => x.Execute(
                @"
delete lst
from List lst
	inner join (SELECT tmp.Id, @rownum := @rownum + 1 AS rankvalue
		  		FROM List tmp, 
       				(SELECT @rownum := 0) r ) ranked on ranked.Id = lst.Id
where lst.Key = @key
    and ranked.rankvalue not between @start and @end",
                new { key = key, start = keepStartingFrom + 1, end = keepEndingAt + 1 }));
        }

        public override void PersistHash(string key)
        {
            Logger.TraceFormat("PersistHash key={0} ", key);

            if (key == null) throw new ArgumentNullException(nameof(key));

            AcquireHashLock();
            QueueCommand(x =>
                x.Execute(
                    "UPDATE MISP.HF_HASH SET EXPIRE_AT = NULL WHERE KEY = :KEY", new { KEY = key }));
        }

        public override void PersistSet(string key)
        {
            Logger.TraceFormat("PersistSet key={0} ", key);

            if (key == null) throw new ArgumentNullException(nameof(key));

            AcquireSetLock();
            QueueCommand(x => x.Execute("UPDATE MISP.HF_SET SET EXPIRE_AT = NULL WHERE KEY = :KEY", new { KEY = key }));
        }

        public override void RemoveSet(string key)
        {
            Logger.TraceFormat("RemoveSet key={0} ", key);

            if (key == null) throw new ArgumentNullException(nameof(key));

            AcquireSetLock();
            QueueCommand(x => x.Execute("DELETE FROM MISP.HF_SET WHERE KEY = :KEY", new { KEY = key }));
        }

        public override void PersistList(string key)
        {
            Logger.TraceFormat("PersistList key={0} ", key);

            if (key == null) throw new ArgumentNullException(nameof(key));

            AcquireListLock();
            QueueCommand(x =>
                x.Execute(
                    "UPDATE MISP.HF_LIST SET EXPIRE_AT = NULL WHERE KEY = :KEY", new { KEY = key }));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Logger.TraceFormat("SetRangeInHash key={0} ", key);

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (keyValuePairs == null)
            {
                throw new ArgumentNullException(nameof(keyValuePairs));
            }

            AcquireHashLock();
            QueueCommand(x =>
                x.Execute(
                    @"
 MERGE INTO MISP.HF_HASH H
      USING (SELECT 1 FROM DUAL) SRC
         ON (H.KEY = :KEY AND H.FIELD = :FIELD)
 WHEN MATCHED THEN
      UPDATE SET VALUE = :VALUE
 WHEN NOT MATCHED THEN
      INSERT (ID, KEY, VALUE, FIELD)
      VALUES (MISP.HF_SEQUENCE.NEXTVAL, :KEY, :VALUE, :FIELD)
",
                    keyValuePairs.Select(y => new { KEY = key, FIELD = y.Key, VALUE = y.Value })));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            Logger.TraceFormat("ExpireHash key={0} ", key);

            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            AcquireHashLock();
            QueueCommand(x =>
                x.Execute(
                    "UPDATE MISP.HF_HASH SET EXPIRE_AT = :EXPIRE_AT WHERE KEY = :KEY",
                    new { KEY = key, EXPIRE_AT = DateTime.UtcNow.Add(expireIn) }));
        }

        public override void RemoveHash(string key)
        {
            Logger.TraceFormat("RemoveHash key={0} ", key);

            if (key == null) throw new ArgumentNullException(nameof(key));

            AcquireHashLock();
            QueueCommand(x => x.Execute(
                "DELETE FROM MISP.HF_HASH WHERE KEY = :KEY", new { KEY = key }));
        }

        public override void Commit()
        {
            _storage.UseTransaction(connection =>
            {
                foreach (var command in _commandQueue)
                {
                    command(connection);
                }
            });
        }

        internal void QueueCommand(Action<IDbConnection> action)
        {
            _commandQueue.Enqueue(action);
        }

        private void AcquireJobLock()
        {
            AcquireLock("Job");
        }

        private void AcquireSetLock()
        {
            AcquireLock("Set");
        }

        private void AcquireListLock()
        {
            AcquireLock("List");
        }

        private void AcquireHashLock()
        {
            AcquireLock("Hash");
        }

        private void AcquireStateLock()
        {
            AcquireLock("State");
        }

        private void AcquireCounterLock()
        {
            AcquireLock("Counter");
        }
        private void AcquireLock(string resource)
        {
        }
    }
}
