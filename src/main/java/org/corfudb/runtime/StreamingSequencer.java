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

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;

import java.util.Set;

/**
 * This is an interface to a stream-aware sequencer.
 */
interface StreamingSequencer
{
    long get_slot(Set<Long> streams);
    long check_tail();
}

/**
 * A trivial implementation of a stream-aware sequencer that passes commands through
 * to the default stream-unaware sequencer.
 */
class CorfuStreamingSequencer implements StreamingSequencer
{
    ClientLib cl;
    public CorfuStreamingSequencer(ClientLib tcl)
    {
        cl = tcl;
    }
    public long get_slot(Set<Long> streams)
    {
        long ret;
        try
        {
            synchronized(cl)
            {
                ret = cl.grabtokens(1);
            }
        }
        catch(CorfuException ce)
        {
            throw new RuntimeException(ce);
        }
        return ret;
    }
    public long check_tail()
    {
        try
        {
            synchronized(cl)
            {
                return cl.querytail();
            }
        }
        catch(CorfuException ce)
        {
            throw new RuntimeException(ce);
        }
    }
}
