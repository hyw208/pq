import os
import json
import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor
from datetime import datetime
import logging
import time
from pydantic import BaseModel
from typing import Any, Dict, Optional

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Job(BaseModel):
    id: int  # SERIAL PRIMARY KEY from DB becomes int
    job_data: Dict[str, Any]  # JSONB column
    created_at: datetime
    claimed_at: Optional[datetime] = None
    done_at: Optional[datetime] = None
    claimed_by: Optional[str] = None
    status: str = "pending"  # Default from DB
    error: Optional[str] = None
    count: int = 0  # Default from DB
    note: Optional[str] = None
    code: int = 0 # For worker

class PersistentQueue:
    def __init__(self, db_config, table_name="job_queue", pool_size=5, max_job_duration=300, max_job_retry=1):
        """
        :param db_config (dict): Database configuration, e.g., {"dbname": "test", "user": "user", "password": "pass", "host": "localhost", "port": "5432"}
        :param table_name: Table name for storing the queue.
        :param pool_size: Size of the connection pool.
        """
        self.db_config = db_config
        self.connection_pool = None
        self.table_name = table_name
        
        self.pool_size = pool_size
        self.max_retries = 3
        self.retry_interval = 2  # seconds

        self.max_job_duration = max_job_duration # seconds
        self.max_job_retry = max_job_retry # 0, 1, 2 etc
        
        # Initialize connection pool
        self._initialize_connection_pool()
        # Ensure the queue table exists in the database
        self._create_table_if_not_exists()

    def _initialize_connection_pool(self):
        """Initialize connection pool with retry logic."""
        for attempt in range(self.max_retries):
            try:
                self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                    1, self.pool_size, cursor_factory=DictCursor, **self.db_config
                )
                if self.connection_pool:
                    logger.info("Successfully initialized database connection pool.")
                    return
            except psycopg2.Error as e:
                logger.error(f"Failed to create connection pool. Retry {attempt}/{self.max_retries}. Error: {e}")
                time.sleep(self.retry_interval)

    def _create_table_if_not_exists(self):
        """Ensures the queue table exists in the database."""
        conn = self._get_connection()
        try:
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id SERIAL PRIMARY KEY,
                    job_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    claimed_at TIMESTAMP,
                    done_at TIMESTAMP,
                    claimed_by TEXT,
                    status TEXT DEFAULT 'pending',
                    error TEXT,
                    count INTEGER DEFAULT 0,
                    note TEXT,
                    code INTEGER DEFAULT 0
                );
                """
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                # conn.commit()
                logger.info(f"Table {self.table_name} has been initialized.")
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Failed to create table: {e}")
        finally:
            self._release_connection(conn)

    def _get_connection(self):
        """Get a connection from the connection pool with retry logic."""
        for attempt in range(self.max_retries):
            try:
                connection = self.connection_pool.getconn()
                connection.autocommit = True
                return connection
            except psycopg2.Error as e:
                logger.error(f"Error getting a connection from the pool: {e}")
                self._initialize_connection_pool()
                time.sleep(2)
        raise Exception("Failed to get a connection from the pool after retrying.")

    def _release_connection(self, connection):
        """ Safely release a connection back to the pool."""
        if connection is None:
            # No connection provided, log warning or take corrective measures if necessary
            logger.error("Attempted to release a None connection.")
            return
        
        try:
            # Attempt to release the connection back to the pool
            self.connection_pool.putconn(connection)
        except Exception as e:
            # Handle unexpected exceptions during connection release
            logger.error(f"Failed to release connection back to pool: {e}")
            self._handle_connection_error(connection)  # Optional: Gracefully handle the connection

    def _handle_connection_error(self, connection):
        """Define additional error handling logic for connection issues."""
        try:
            # Optional step: Close the connection if it can't be returned to the pool.
            connection.close()
        except Exception as close_error:
            # Log the failure to close the connection
            self._log_error(f"Failed to close the faulty connection: {close_error}")
            
    def add_job(self, job_data):
        """
        Add a job to the queue.

        :param job_data: Text data representing the job details to be added.
        """
        
        job_data_str = json.dumps(job_data)
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {self.table_name} (job_data) VALUES (%s) RETURNING id;", (job_data_str,))
                job_id = cursor.fetchone()["id"]
                logger.info(f"Added job with ID {job_id}.")
                return job_id
            
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Failed to enqueue job: {e}")
        finally:
            self._release_connection(conn)

    def claim_next_job(self, worker):
        """
        Claim the next available job in a FIFO manner.

        :param worker: The name of the worker claiming the job.
        :return: Dictionary containing the job details, or None if no jobs are available.

        1) never claimed/picked up: claimed_at IS NULL
        2) claimed but not done in time and below retry count: 
        3) clamied, done in time but not successful and below retry count

        status: pending | claimed | success | failure
        """
        select_query = f"""
        SELECT * FROM {self.table_name}
        WHERE 
            (
                claimed_at IS NULL
            )
            OR 
            (
                (
                    claimed_at IS NOT NULL AND done_at IS NULL
                ) 
                AND 
                (
                    NOW() - claimed_at > INTERVAL '{self.max_job_duration} seconds'
                )
                AND
                (
                    count < {self.max_job_retry}
                )
            )
            OR 
            (
                (
                    done_at IS NOT NULL AND status <> 'success'
                ) 
                AND 
                (
                    count < {self.max_job_retry}
                )
            )
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        """

        update_query = f"""
        UPDATE {self.table_name}
        SET claimed_at = NOW(), claimed_by = %s, status = 'claimed', count = count + 1, note = %s
        WHERE id = (
            SELECT id FROM {self.table_name}
            WHERE 
                (
                    claimed_at IS NULL
                )
                OR 
                (
                    (
                        claimed_at IS NOT NULL AND done_at IS NULL
                    ) 
                    AND 
                    (
                        NOW() - claimed_at > INTERVAL '{self.max_job_duration} seconds'
                    )
                    AND
                    (
                        count < {self.max_job_retry}
                    )
                )
                OR 
                (
                    (
                        done_at IS NOT NULL AND status <> 'success'
                    ) 
                    AND 
                    (
                        count < {self.max_job_retry}
                    )
                )
            ORDER BY created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING *;
        """
        conn = self._get_connection()
        conn.autocommit = False
        try:
            with conn.cursor() as cursor:
                cursor.execute(select_query)
                record = cursor.fetchone()
                if not record: # no job to claim
                    return None 
                
                job = Job(**dict(record)) # pydantic Job model
                now = datetime.now()
                note = job.note + " \n " if job.note else ""
                note += f"[action(claim job),dt({now}),worker({worker}),status(),error(),note(),code()]"
                
                cursor.execute(update_query, (worker, note))
                record = cursor.fetchone()
                if not record: # 
                    raise RuntimeError(f"Failed to claim job({job.id})")
                
                job = Job(**dict(record)) # pydantic Job model
                conn.commit()
                return job
                
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Failed to enqueue job: {e}")
        finally:
            self._release_connection(conn)

    def complete_job(self, job_id, worker, status, error=None, job_data=None):
        """
        Mark a job as completed.
        """
        select_query = f"""
        SELECT * FROM {self.table_name}
        WHERE id = %s
        FOR UPDATE
        """
        conn = self._get_connection()
        conn.autocommit = False
        try: 
            with conn.cursor() as cursor:
                cursor.execute(select_query, (job_id,))
                record = cursor.fetchone()
                if not record:
                    raise ValueError(f"Failed to locate the job({job_id})")
            
                job = Job(**dict(record)) # pydantic Job model
                now = datetime.now()

                # if the job somehow has been done successfully, should we update? no, leave a note and let worker know to rollback?
                if job.done_at and job.status == 'success': 
                    update_query = f"""
                    UPDATE {self.table_name}
                    SET note = %s, code = %s
                    WHERE id = %s
                    RETURNING *;
                    """
                    code = 409 # worker should rollback its work...
                    comment = "somehow job has already been done successfully, just leaving a note ..."
                    note = job.note + " \n " if job.note else ""
                    note += f"[action(complete job),dt({now}),worker({worker}),status({status}),error({error}),note({comment}),code({code})]"

                    cursor.execute(update_query, (note, code, job_id))
                    record = cursor.fetchone()
                    if not record:
                        raise RuntimeError(f"Failed to complete the job({job_id}) which has already been done successfully")
                    
                    job = Job(**dict(record)) 
                    conn.commit()
                    return job

                update_query = f"""
                UPDATE {self.table_name}
                SET done_at = NOW(), claimed_by = %s, status = %s, error = %s, note = %s, job_data = %s, code = %s
                WHERE id = %s
                RETURNING *;
                """
                if (now - job.claimed_at).total_seconds() > self.max_job_duration: 
                    #a.1 late but no one else picked up by looking at worker field, go ahead and update it, update note saying worker is late
                    #a.2 late and picked up by a new worker, well still go ahead and update it to save time, leave a note    
                    code = 202 if job.claimed_by == worker else 203
                    comment = f"worker({worker}) is late but job's not claimed by other" if job.claimed_by == worker else f"worker({worker}) is late and job's claimed by other({job.claimed_by})"
                
                else: 
                    #b.1 current worker is not late and job not picked up by prior/new worker, go ahead and update it
                    #b.2 current worker is not late but prior worker update before current worker, but go ahead and update
                    code = 200 if job.claimed_by == worker else 201
                    comment = f"worker({worker}) completed in time" if job.claimed_by == worker else f"worker({worker}) completed in time but prior worker({job.claimed_by}) completed right before current"

                note = job.note + " \n " if job.note else ""
                note += f"[action(complete job),dt({now}),worker({worker}),status({status}),error({error}),note({comment}),code({code})]"
                job_data = job_data | job.job_data # job.job_data overrides job_data
                job_data_str = json.dumps(job_data)
            
                cursor.execute(update_query, (worker, status, error, note, job_data_str, code, job_id))
                record = cursor.fetchone()
                if not record:
                    raise RuntimeError(f"Failed to locate the job with job id: {job_id}")
                
                job = Job(**dict(record)) 
                conn.commit()
                return job

        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Failed to complete a job: {e}")
        finally:
            self._release_connection(conn)

