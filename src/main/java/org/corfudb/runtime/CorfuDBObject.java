/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.runtime;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This class must be sub-classed by any new CorfuDB object class. It provides
 * helper code for locking and timestamp bookkeeping. It also defines the abstract
 * apply upcall that every CorfuDB object must implement.
 */
public abstract class CorfuDBObject
{
    //object ID -- corresponds to stream ID used underneath
    long oid;
    ReadWriteLock maplock;
    AbstractRuntime TR;

    AtomicLong timestamp;

    public long getTimestamp()
    {
        return timestamp.get();
    }

    public void setTimestamp(long newts)
    {
        timestamp.set(newts);
    }

    public void lock()
    {
        lock(false);
    }

    public void unlock()
    {
        unlock(false);
    }

    public void lock(boolean write)
    {
        if(write) maplock.writeLock().lock();
        else maplock.readLock().lock();
    }

    public void unlock(boolean write)
    {
        if(write) maplock.writeLock().unlock();
        else maplock.readLock().unlock();
    }

    abstract public void apply(Object update);

    public long getID()
    {
        return oid;
    }

    public CorfuDBObject(AbstractRuntime tTR, long tobjectid)
    {
        TR = tTR;
        oid = tobjectid;
        timestamp = new AtomicLong();
    }

}
