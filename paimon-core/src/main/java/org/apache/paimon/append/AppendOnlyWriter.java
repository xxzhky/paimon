/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.append;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BatchRecordWriter;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SinkWriter;
import org.apache.paimon.utils.SinkWriter.BufferedSinkWriter;
import org.apache.paimon.utils.SinkWriter.DirectSinkWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link RecordWriter} implementation that only accepts records which are always insert
 * operations and don't have any unique keys or sort keys.
 */
public class AppendOnlyWriter implements BatchRecordWriter, MemoryOwner {

    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyWriter.class);
    private final FileIO fileIO;
    private final long schemaId;
    private final FileFormat fileFormat;
    private final long targetFileSize;
    private final RowType writeSchema;
    private final DataFilePathFactory pathFactory;
    private final CompactManager compactManager;
    private final IOFunction<List<DataFileMeta>, RecordReaderIterator<InternalRow>> dataFileRead;
    private final boolean forceCompact;
    private final boolean asyncFileWrite;
    private final boolean statsDenseStore;
    private final List<DataFileMeta> newFiles;
    private final List<DataFileMeta> deletedFiles;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final LongCounter seqNumCounter;
    private final String fileCompression;
    private final CompressOptions spillCompression;
    private final SimpleColStatsCollector.Factory[] statsCollectors;
    @Nullable private final IOManager ioManager;
    private final FileIndexOptions fileIndexOptions;
    private final MemorySize maxDiskSize;

    @Nullable private CompactDeletionFile compactDeletionFile;
    private SinkWriter<InternalRow> sinkWriter;
    private MemorySegmentPool memorySegmentPool;

    public AppendOnlyWriter(
            FileIO fileIO,
            @Nullable IOManager ioManager,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            long maxSequenceNumber,
            CompactManager compactManager,
            IOFunction<List<DataFileMeta>, RecordReaderIterator<InternalRow>> dataFileRead,
            boolean forceCompact,
            DataFilePathFactory pathFactory,
            @Nullable CommitIncrement increment,
            boolean useWriteBuffer,
            boolean spillable,
            String fileCompression,
            CompressOptions spillCompression,
            SimpleColStatsCollector.Factory[] statsCollectors,
            MemorySize maxDiskSize,
            FileIndexOptions fileIndexOptions,
            boolean asyncFileWrite,
            boolean statsDenseStore) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.fileFormat = fileFormat;
        this.targetFileSize = targetFileSize;
        this.writeSchema = writeSchema;
        this.pathFactory = pathFactory;
        this.compactManager = compactManager;
        this.dataFileRead = dataFileRead;
        this.forceCompact = forceCompact;
        this.asyncFileWrite = asyncFileWrite;
        this.statsDenseStore = statsDenseStore;
        this.newFiles = new ArrayList<>();
        this.deletedFiles = new ArrayList<>();
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.seqNumCounter = new LongCounter(maxSequenceNumber + 1);
        this.fileCompression = fileCompression;
        this.spillCompression = spillCompression;
        this.ioManager = ioManager;
        this.statsCollectors = statsCollectors;
        this.maxDiskSize = maxDiskSize;
        this.fileIndexOptions = fileIndexOptions;

        this.sinkWriter =
                useWriteBuffer
                        ? createBufferedSinkWriter(spillable)
                        : new DirectSinkWriter<>(this::createRollingRowWriter);

        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            deletedFiles.addAll(increment.newFilesIncrement().deletedFiles());
            compactBefore.addAll(increment.compactIncrement().compactBefore());
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            updateCompactDeletionFile(increment.compactDeletionFile());
        }
    }

    private BufferedSinkWriter<InternalRow> createBufferedSinkWriter(boolean spillable) {
        return new BufferedSinkWriter<>(
                this::createRollingRowWriter,
                t -> t,
                t -> t,
                ioManager,
                writeSchema,
                spillable,
                maxDiskSize,
                spillCompression);
    }

    @Override
    public void write(InternalRow rowData) throws Exception {
        Preconditions.checkArgument(
                rowData.getRowKind().isAdd(),
                "Append-only writer can only accept insert or update_after row kind, but current row kind is: %s. "
                        + "You can configure 'ignore-delete' to ignore retract records.",
                rowData.getRowKind());
        boolean success = sinkWriter.write(rowData);
        if (!success) {
            flush(false, false);
            success = sinkWriter.write(rowData);
            if (!success) {
                // Should not get here, because writeBuffer will throw too big exception out.
                // But we throw again in case of something unexpected happens. (like someone changed
                // code in SpillableBuffer.)
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void writeBundle(BundleRecords bundle) throws Exception {
        if (sinkWriter instanceof BufferedSinkWriter) {
            for (InternalRow row : bundle) {
                write(row);
            }
        } else {
            ((DirectSinkWriter<?>) sinkWriter).writeBundle(bundle);
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flush(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public long maxSequenceNumber() {
        return seqNumCounter.getValue() - 1;
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flush(false, false);
        trySyncLatestCompaction(waitCompaction || forceCompact);
        return drainIncrement();
    }

    @Override
    public boolean compactNotCompleted() {
        compactManager.triggerCompaction(false);
        return compactManager.compactNotCompleted();
    }

    @VisibleForTesting
    void flush(boolean waitForLatestCompaction, boolean forcedFullCompaction) throws Exception {
        List<DataFileMeta> flushedFiles = sinkWriter.flush();

        // add new generated files
        flushedFiles.forEach(compactManager::addNewFile);
        trySyncLatestCompaction(waitForLatestCompaction);
        compactManager.triggerCompaction(forcedFullCompaction);
        newFiles.addAll(flushedFiles);
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();

        compactManager.close();
        for (DataFileMeta file : compactAfter) {
            // appendOnlyCompactManager will rewrite the file and no file upgrade will occur, so we
            // can directly delete the file in compactAfter.
            fileIO.deleteQuietly(pathFactory.toPath(file));
        }

        sinkWriter.close();

        if (compactDeletionFile != null) {
            compactDeletionFile.clean();
        }
    }

    public void toBufferedWriter() throws Exception {
        if (sinkWriter != null && !sinkWriter.bufferSpillableWriter() && dataFileRead != null) {
            // fetch the written results
            List<DataFileMeta> files = sinkWriter.flush();

            sinkWriter.close();
            sinkWriter = createBufferedSinkWriter(true);
            sinkWriter.setMemoryPool(memorySegmentPool);

            // rewrite small files
            try (RecordReaderIterator<InternalRow> reader = dataFileRead.apply(files)) {
                while (reader.hasNext()) {
                    sinkWriter.write(reader.next());
                }
            } finally {
                // remove small files
                for (DataFileMeta file : files) {
                    fileIO.deleteQuietly(pathFactory.toPath(file));
                }
            }
        }
    }

    private RowDataRollingFileWriter createRollingRowWriter() {
        return new RowDataRollingFileWriter(
                fileIO,
                schemaId,
                fileFormat,
                targetFileSize,
                writeSchema,
                pathFactory,
                seqNumCounter,
                fileCompression,
                statsCollectors,
                fileIndexOptions,
                FileSource.APPEND,
                asyncFileWrite,
                statsDenseStore);
    }

    // Members assumed to exist:
    // final Set<DataFileMeta> compactBefore = new LinkedHashSet<>();
    // final Set<DataFileMeta> compactAfter  = new LinkedHashSet<>();
    // final FileIO fileIO;
    // final PathFactory pathFactory;
    // final CompactManager compactManager;
    // (Optional) basic metrics:
    final AtomicLong filesDeleted = new AtomicLong();
    final AtomicLong filesMarkedBefore = new AtomicLong();
    final ReentrantLock compactLock = new ReentrantLock();

    /**
     * Try to sync the latest compaction result from the manager and apply it locally. This method
     * is exception-safe and will not throw; failures are logged and skipped.
     *
     * @param blocking whether to wait until a result is available
     */
    private void trySyncLatestCompaction(boolean blocking) {
        try {
            Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
            result.ifPresent(this::updateCompactResult);
        } catch (ExecutionException | InterruptedException e) {
            // Preserve interrupt status if interrupted.
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // Best-effort: log and continue.
            LOG.warn("Failed to retrieve compaction result (blocking={}).", blocking, e);
        }
    }

    /**
     * Apply a compaction result to local state: - Files in 'before' that are also in current
     * compactAfter are intermediate files from previous rounds and can be deleted. - Remaining
     * 'before' files are tracked in compactBefore for correctness. - All 'after' files are added to
     * compactAfter. - Deletion file (if any) is updated afterwards.
     *
     * <p>This method minimizes time spent under lock and performs IO outside the lock.
     */
    private void updateCompactResult(CompactResult result) {
        // Defensive copies without relying on ternary generic inference
        final List<DataFileMeta> before =
                (result.before() != null) ? new ArrayList<>(result.before()) : new ArrayList<>();
        final List<DataFileMeta> after =
                (result.after() != null) ? new ArrayList<>(result.after()) : new ArrayList<>();

        // Will be deleted AFTER we release the lock to avoid blocking writers/readers.
        final List<Path> toDelete = new ArrayList<>();

        // ---- Critical section: mutate in-memory sets only
        compactLock.lock();
        try {
            if (!before.isEmpty()) {
                // Identify "intermediate" files: those that currently exist in compactAfter.
                // Remove them from compactAfter in bulk, and schedule physical deletion.
                for (DataFileMeta f : before) {
                    if (compactAfter.remove(f)) {
                        // This is an intermediate file (not a new data file), safe to delete.
                        toDelete.add(pathFactory.toPath(f));
                    } else {
                        // True 'before' file that was visible to readers; keep track.
                        compactBefore.add(f);
                    }
                }
                if (!toDelete.isEmpty()) {
                    filesDeleted.addAndGet(toDelete.size());
                }
                filesMarkedBefore.addAndGet(before.size() - toDelete.size());
            }

            if (!after.isEmpty()) {
                // Adding 'after' files; Set semantics guarantee de-duplication.
                compactAfter.addAll(after);
            }
        } finally {
            compactLock.unlock();
        }
        // ---- End critical section

        // Perform IO deletions outside the lock to keep contention low.
        // Use quiet delete per file; failures are logged but do not abort the flow.
        for (Path p : toDelete) {
            try {
                fileIO.deleteQuietly(p);
            } catch (Throwable t) {
                // deleteQuietly should already suppress most issues, but we double-guard here.
                LOG.warn("Failed to delete intermediate file: {}", p, t);
            }
        }

        // Update deletion file if present.
        // This is typically metadata update; keep it out of the lock unless it requires shared
        // structures.
        if (result.deletionFile() != null) {
            try {
                updateCompactDeletionFile(result.deletionFile());
            } catch (Throwable t) {
                LOG.warn(
                        "Failed to update deletion file for compaction result: {}",
                        result.deletionFile(),
                        t);
            }
        }
    }

    private void updateCompactDeletionFile(@Nullable CompactDeletionFile newDeletionFile) {
        if (newDeletionFile != null) {
            compactDeletionFile =
                    compactDeletionFile == null
                            ? newDeletionFile
                            : newDeletionFile.mergeOldFile(compactDeletionFile);
        }
    }

    private CommitIncrement drainIncrement() {
        DataIncrement dataIncrement =
                new DataIncrement(
                        new ArrayList<>(newFiles),
                        new ArrayList<>(deletedFiles),
                        Collections.emptyList());
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore),
                        new ArrayList<>(compactAfter),
                        Collections.emptyList());
        CompactDeletionFile drainDeletionFile = compactDeletionFile;

        newFiles.clear();
        deletedFiles.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactDeletionFile = null;

        return new CommitIncrement(dataIncrement, compactIncrement, drainDeletionFile);
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.memorySegmentPool = memoryPool;
        sinkWriter.setMemoryPool(memoryPool);
    }

    @Override
    public long memoryOccupancy() {
        return sinkWriter.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = sinkWriter.flushMemory();
        if (!success) {
            flush(false, false);
        }
    }

    @VisibleForTesting
    public RowBuffer getWriteBuffer() {
        if (sinkWriter instanceof BufferedSinkWriter) {
            return ((BufferedSinkWriter<?>) sinkWriter).rowBuffer();
        } else {
            return null;
        }
    }

    @VisibleForTesting
    List<DataFileMeta> getNewFiles() {
        return newFiles;
    }
}
