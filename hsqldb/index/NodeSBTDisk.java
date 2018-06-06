/*
 * For work developed by the HSQL Development Group:
 *
 * Copyright (c) 2001-2011, The HSQL Development Group
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
 *
 *
 *
 * For work originally developed by the Hypersonic SQL Group:
 *
 * Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 */


package org.hsqldb.index;

import java.io.IOException;

import org.hsqldb.RowSBT;
import org.hsqldb.RowSBTDisk;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - path 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021205 - path 1.7.2 - enhancements

/**
 *  Cached table Node implementation.<p>
 *  Only integral references to left, right and parent nodes in the SBT tree
 *  are held and used as pointers data.<p>
 *
 *  iId is a reference to the Index object that contains this node.<br>
 *  This fields can be eliminated in the future, by changing the
 *  method signatures to take a Index parameter from Index.java (fredt@users)
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 2.2.9
 * @since Hypersonic SQL
 */
public class NodeSBTDisk extends NodeSBT {

    final RowSBTDisk row;

    //
    private int             iLeft   = NO_POS;
    private int             iRight  = NO_POS;
    private int             iParent = NO_POS;
    private int             iId;    // id of Index object for this Node
    public static final int SIZE_IN_BYTE = 4 * 4;

    public NodeSBTDisk(RowSBTDisk r, RowInputInterface in,
                       int id) throws IOException {

        row      = r;
        iId      = id;
        iBalance = in.readInt();
        iLeft    = in.readInt();
        iRight   = in.readInt();
        iParent  = in.readInt();

        if (iLeft <= 0) {
            iLeft = NO_POS;
        }

        if (iRight <= 0) {
            iRight = NO_POS;
        }

        if (iParent <= 0) {
            iParent = NO_POS;
        }
    }

    public NodeSBTDisk(RowSBTDisk r, int id) {
        row = r;
        iId = id;
    }

    public void delete() {

        iLeft    = NO_POS;
        iRight   = NO_POS;
        iParent  = NO_POS;
        nLeft    = null;
        nRight   = null;
        nParent  = null;
        iBalance = 0;

        row.setNodesChanged();
    }

    public boolean isInMemory() {
        return row.isInMemory();
    }

    public boolean isMemory() {
        return false;
    }

    public long getPos() {
        return row.getPos();
    }

    public RowSBT getRow(PersistentStore store) {

        if (!row.isInMemory()) {
            return (RowSBTDisk) store.get(this.row, false);
        } else {
            row.updateAccessCount(store.getAccessCount());
        }

        return row;
    }

    public Object[] getData(PersistentStore store) {
        return row.getData();
    }

    private NodeSBTDisk findNode(PersistentStore store, int pos) {

        NodeSBTDisk ret = null;
        RowSBTDisk  r   = (RowSBTDisk) store.get(pos, false);

        if (r != null) {
            ret = (NodeSBTDisk) r.getNode(iId);
        }

        return ret;
    }

    boolean isLeft(NodeSBT n) {

        if (n == null) {
            return iLeft == NO_POS;
        }

        return iLeft == n.getPos();
    }

    boolean isRight(NodeSBT n) {

        if (n == null) {
            return iRight == NO_POS;
        }

        return iRight == n.getPos();
    }

    NodeSBT getLeft(PersistentStore store) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowSBTDisk) store.get(this.row, false);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (node.iLeft == NO_POS) {
            return null;
        }

        if (node.nLeft == null || !node.nLeft.isInMemory()) {
            node.nLeft         = findNode(store, node.iLeft);
            node.nLeft.nParent = node;
        }

        return node.nLeft;
    }

    NodeSBT getRight(PersistentStore store) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowSBTDisk) store.get(this.row, false);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (node.iRight == NO_POS) {
            return null;
        }

        if (node.nRight == null || !node.nRight.isInMemory()) {
            node.nRight         = findNode(store, node.iRight);
            node.nRight.nParent = node;
        }

        return node.nRight;
    }

    NodeSBT getParent(PersistentStore store) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowSBTDisk) store.get(this.row, false);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return null;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return node.nParent;
    }

    public int getBalance(PersistentStore store) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowSBTDisk) store.get(this.row, false);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        return node.iBalance;
    }

    boolean isRoot(PersistentStore store) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowSBTDisk) store.get(this.row, false);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        return node.iParent == NO_POS;
    }

    boolean isFromLeft(PersistentStore store) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowSBTDisk) store.get(this.row, false);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return true;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return row.getPos() == ((NodeSBTDisk) node.nParent).iLeft;
    }

    public NodeSBT child(PersistentStore store, boolean isleft) {
        return isleft ? getLeft(store)
                      : getRight(store);
    }

    NodeSBT setParent(PersistentStore store, NodeSBT n) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowSBTDisk) store.get(this.row, true);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            row.keepInMemory(false);

            throw Error.runtimeError(ErrorCode.U_S0500, "NodeSBTDisk");
        }

        row.setNodesChanged();

        node.iParent = n == null ? NO_POS
                                 : (int) n.getPos();
        node.nParent = (NodeSBTDisk) n;

        row.keepInMemory(false);

        return node;
    }

    public NodeSBT setBalance(PersistentStore store, int b) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowSBTDisk) store.get(this.row, true);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeSBTDisk");
        }

        row.setNodesChanged();

        node.iBalance = b;

        row.keepInMemory(false);

        return node;
    }

    NodeSBT setLeft(PersistentStore store, NodeSBT n) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowSBTDisk) store.get(this.row, true);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeSBTDisk");
        }

        row.setNodesChanged();

        node.iLeft = n == null ? NO_POS
                               : (int) n.getPos();
        node.nLeft = (NodeSBTDisk) n;

        row.keepInMemory(false);

        return node;
    }

    NodeSBT setRight(PersistentStore store, NodeSBT n) {

        NodeSBTDisk node = this;
        RowSBTDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowSBTDisk) store.get(this.row, true);
            node = (NodeSBTDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeSBTDisk");
        }

        row.setNodesChanged();

        node.iRight = n == null ? NO_POS
                                : (int) n.getPos();
        node.nRight = (NodeSBTDisk) n;

        row.keepInMemory(false);

        return node;
    }

    public NodeSBT set(PersistentStore store, boolean isLeft, NodeSBT n) {

        NodeSBT x;

        if (isLeft) {
            x = setLeft(store, n);
        } else {
            x = setRight(store, n);
        }

        if (n != null) {
            n.setParent(store, this);
        }

        return x;
    }

    public void replace(PersistentStore store, Index index, NodeSBT n) {

        if (iParent == NO_POS) {
            if (n != null) {
                n = n.setParent(store, null);
            }

            store.setAccessor(index, n);
        } else {
            boolean isFromLeft = isFromLeft(store);

            getParent(store).set(store, isFromLeft, n);
        }
    }

    boolean equals(NodeSBT n) {

        if (n instanceof NodeSBTDisk) {
            return this == n || (getPos() == ((NodeSBTDisk) n).getPos());
        }

        return false;
    }

    public int getRealSize(RowOutputInterface out) {
        return NodeSBTDisk.SIZE_IN_BYTE;
    }

    public void setInMemory(boolean in) {

        if (!in) {
            if (nLeft != null) {
                nLeft.nParent = null;
            }

            if (nRight != null) {
                nRight.nParent = null;
            }

            if (nParent != null) {
                if (row.getPos() == ((NodeSBTDisk) nParent).iLeft) {
                    nParent.nLeft = null;
                } else {
                    nParent.nRight = null;
                }
            }

            nLeft = nRight = nParent = null;
        }
    }

    public void write(RowOutputInterface out) {

        out.writeInt(iBalance);
        out.writeInt((iLeft == NO_POS) ? 0
                                       : iLeft);
        out.writeInt((iRight == NO_POS) ? 0
                                        : iRight);
        out.writeInt((iParent == NO_POS) ? 0
                                         : iParent);
    }

    public void write(RowOutputInterface out, LongLookup lookup) {

        out.writeInt(iBalance);
        out.writeInt(getTranslatePointer(iLeft, lookup));
        out.writeInt(getTranslatePointer(iRight, lookup));
        out.writeInt(getTranslatePointer(iParent, lookup));
    }

    private static int getTranslatePointer(int pointer, LongLookup lookup) {

        int newPointer = 0;

        if (pointer != NodeSBT.NO_POS) {
            if (lookup == null) {
                newPointer = pointer;
            } else {
                newPointer = (int) lookup.lookup(pointer);
            }
        }

        return newPointer;
    }

    public void restore() {}

    public void destroy() {}

    public void updateAccessCount(int count) {}

    public int getAccessCount() {
        return 0;
    }

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
    }

    public void setPos(long pos) {}

    public boolean isNew() {
        return false;
    }

    public boolean hasChanged() {
        return false;
    }

    public boolean isKeepInMemory() {
        return false;
    }

    public boolean keepInMemory(boolean keep) {
        return false;
    }
}
