/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.persist;

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowSBT;
import org.hsqldb.RowSBTDisk;
import org.hsqldb.RowAction;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.index.IndexSBT;
import org.hsqldb.index.NodeSBT;
import org.hsqldb.index.NodeSBTDisk;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;

/*
 * Implementation of PersistentStore for result sets.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class RowStoreSBTHybrid extends RowStoreSBT implements PersistentStore {

    DataFileCache   cache;
    private int     maxMemoryRowCount;
    private boolean useDisk;
    boolean         isCached;
    int             rowIdSequence = 0;

    public RowStoreSBTHybrid(Session session,
                             PersistentStoreCollection manager,
                             TableBase table, boolean diskBased) {

        this.manager           = manager;
        this.table             = table;
        this.maxMemoryRowCount = session.getResultMemoryRowCount();
        this.useDisk           = diskBased;

        if (maxMemoryRowCount == 0) {
            this.useDisk = false;
        }

        if (table.getTableType() == TableBase.RESULT_TABLE) {
            setTimestamp(session.getActionTimestamp());
        }

// test code to force use of cache
/*
        if (diskBased) {
            this.maxMemoryRowCount = 0;
            this.useDisk           = true;
        }
*/

//
        resetAccessorKeys(session, table.getIndexList());
        manager.setStore(table, this);

        nullsList = new boolean[table.getColumnCount()];
    }

    public boolean isMemory() {
        return !isCached;
    }

    public void setMemory(boolean mode) {
        useDisk = !mode;
    }

    public synchronized int getAccessCount() {
        return isCached ? cache.getAccessCount()
                        : 0;
    }

    public void set(CachedObject object) {}

    public CachedObject get(long i) {

        try {
            if (isCached) {
                return cache.get(i, this, false);
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "RowStoreSBTHybrid");
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(long i, boolean keep) {

        try {
            if (isCached) {
                return cache.get(i, this, keep);
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "RowStoreSBTHybrid");
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public CachedObject get(CachedObject object, boolean keep) {

        try {
            if (isCached) {
                return cache.get(object, this, keep);
            } else {
                return object;
            }
        } catch (HsqlException e) {
            return null;
        }
    }

    public void add(Session session, CachedObject object, boolean tx) {

        if (isCached) {
            int size = object.getRealSize(cache.rowOut);

            size += indexList.length * NodeSBTDisk.SIZE_IN_BYTE;
            size = cache.rowOut.getStorageSize(size);

            object.setStorageSize(size);

            long pos = tableSpace.getFilePosition(size, false);

            object.setPos(pos);
            cache.add(object);
        }

        Object[] data = ((Row) object).getData();

        for (int i = 0; i < nullsList.length; i++) {
            if (data[i] == null) {
                nullsList[i] = true;
            }
        }
    }

    public CachedObject get(RowInputInterface in) {

        try {
            if (isCached) {
                return new RowSBTDisk(table, in);
            }
        } catch (HsqlException e) {
            return null;
        } catch (IOException e1) {
            return null;
        }

        return null;
    }

    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {

        if (!isCached) {
            if (useDisk && elementCount.get() >= maxMemoryRowCount) {
                changeToDiskTable(session);
            }
        }

        Row row;

        if (isCached) {
            row = new RowSBTDisk(table, (Object[]) object, this);
        } else {
            int id = rowIdSequence++;

            row = new RowSBT(table, (Object[]) object, id, this);
        }

        add(session, row, tx);

        return row;
    }

    public void indexRow(Session session, Row row) {

        try {
            row = (Row) get(row, true);

            super.indexRow(session, row);
            row.keepInMemory(false);
        } catch (HsqlException e) {
            throw e;
        }
    }

    public void removeAll() {

        if (!isCached) {
            destroy();
        }

        elementCount.set(0);
        ArrayUtil.fillArray(accessorList, null);

        for (int i = 0; i < nullsList.length; i++) {
            nullsList[i] = false;
        }
    }

    public void remove(CachedObject object) {

        if (isCached) {
            cache.remove(object);
        }
    }

    public void commitPersistence(CachedObject row) {}

    public void commitRow(Session session, Row row, int changeAction,
                          int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                remove(row);
                break;

            case RowAction.ACTION_INSERT :
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELEETE
                remove(row);
                break;

            case RowAction.ACTION_DELETE_FINAL :
                delete(session, row);
                remove(row);
                break;
        }
    }

    public void rollbackRow(Session session, Row row, int changeAction,
                            int txModel) {

        switch (changeAction) {

            case RowAction.ACTION_DELETE :
                row = (Row) get(row, true);

                ((RowSBT) row).setNewNodes(this);
                row.keepInMemory(false);
                indexRow(session, row);
                break;

            case RowAction.ACTION_INSERT :
                delete(session, row);
                remove(row);
                break;

            case RowAction.ACTION_INSERT_DELETE :

                // INSERT + DELEETE
                remove(row);
                break;
        }
    }

    //
    public DataFileCache getCache() {
        return cache;
    }

    public void setCache(DataFileCache cache) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreSBTHybrid");
    }

    public void release() {

        if (!isCached) {
            destroy();
        }

        ArrayUtil.fillArray(accessorList, null);

        if (isCached) {
            cache.adjustStoreCount(-1);

            cache    = null;
            isCached = false;
        }

        manager.setStore(table, null);
        elementCount.set(0);
    }

    public void delete(Session session, Row row) {
        super.delete(session, row);
    }

    public CachedObject getAccessor(Index key) {

        NodeSBT node = (NodeSBT) accessorList[key.getPosition()];

        if (node == null) {
            return null;
        }

        RowSBT row = (RowSBT) get(node.getRow(this), false);

        node                            = row.getNode(key.getPosition());
        accessorList[key.getPosition()] = node;

        return node;
    }

    public synchronized void resetAccessorKeys(Session session, Index[] keys) {

        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];

            return;
        }

        if (isCached) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreSBTHybrid");
        }

        super.resetAccessorKeys(session, keys);
    }

    public boolean hasNull(int pos) {
        return nullsList[pos];
    }

    public final void changeToDiskTable(Session session) {

        cache =
            ((PersistentStoreCollectionSession) manager).getSessionDataCache();

        if (cache != null) {
            tableSpace = cache.spaceManager.getTableSpace(
                DataSpaceManager.tableIdDefault);

            IndexSBT    idx      = (IndexSBT) indexList[0];
            NodeSBT     root     = (NodeSBT) accessorList[0];
            RowIterator iterator = table.rowIterator(this);

            ArrayUtil.fillArray(accessorList, null);
            elementCount.set(0);

            isCached = true;

            cache.adjustStoreCount(1);

            while (iterator.hasNext()) {
                Row row = iterator.getNextRow();
                Row newRow = (Row) getNewCachedObject(session, row.getData(),
                                                      false);

                indexRow(session, newRow);
            }

            idx.unlinkNodes(root);
        }

        maxMemoryRowCount = Integer.MAX_VALUE;
    }
}
