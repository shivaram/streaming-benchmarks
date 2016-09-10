package flink.benchmark;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;

/**
 * Base class for LoadGenerator Sources.  Implements features to generate whatever load
 * you require.
 */
public abstract class LoadGeneratorSource<T> extends RichParallelSourceFunction<T> implements Checkpointed<Long> {

  private volatile boolean running = true;

  private final int loadTargetHz;
  private final int timeSliceLengthMs;
  private final long totalEventsToGenerate;
  private long beginTs;

  private long elementsGenerated = 0;

  public LoadGeneratorSource(int loadTargetHz, int timeSliceLengthMs, long totalEventsToGenerate) {
    this.loadTargetHz = loadTargetHz;
    this.timeSliceLengthMs = timeSliceLengthMs;
    this.totalEventsToGenerate = totalEventsToGenerate;
    this.beginTs = System.currentTimeMillis();
  }

  /**
   * Subclasses must override this to generate a data element
   */
  public abstract T generateElement(long sliceTs);


  /**
   * The main loop
   */
  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    final Object checkpointLock = sourceContext.getCheckpointLock();

    int elements = loadPerTimeslice();
    long totalElementsPerTask = 
      totalEventsToGenerate / getRuntimeContext().getNumberOfParallelSubtasks();
    if (elementsGenerated == 0) {
      System.out.println("OLD TS WAS " + beginTs + " NEW IS " + System.currentTimeMillis());
      beginTs = System.currentTimeMillis();
    }

    while (running && elementsGenerated < totalElementsPerTask) {
      long emitStartTime = System.currentTimeMillis();
      long sliceTs = beginTs + (timeSliceLengthMs * (elementsGenerated / elements));
      synchronized (checkpointLock) {
        System.out.println("SLICE TS IS " + sliceTs +
            " elementsRatio " + elementsGenerated/elements +
            " CUR TS IS " + emitStartTime +
            " diff " + (emitStartTime - sliceTs));
        for (int i = 0; i < elements; i++) {
          sourceContext.collect(generateElement(sliceTs));
        }
        elementsGenerated += elements;
      }
      // Sleep for the rest of timeslice if needed
      // long emitTime = System.currentTimeMillis() - emitStartTime;
      long emitEndTime = System.currentTimeMillis();
      if (emitEndTime < (sliceTs + timeSliceLengthMs)) {
        System.out.println("SLEEPING for " + (sliceTs + timeSliceLengthMs - emitEndTime));
        Thread.sleep(sliceTs + timeSliceLengthMs - emitEndTime);
      } else {
        System.out.println("FALLING BEHIND by " + (emitEndTime - sliceTs - timeSliceLengthMs));
      }
    }
    sourceContext.close();
  }

  @Override
  public void cancel() {
    running = false;
  }

  /**
   * Given a desired load figure out how many elements to generate in each timeslice
   * before yielding for the rest of that timeslice
   */
  private int loadPerTimeslice() {
    int messagesPerOperator = loadTargetHz / getRuntimeContext().getNumberOfParallelSubtasks();
    return messagesPerOperator / (1000 / timeSliceLengthMs);
  }

  @Override
  public Long snapshotState(long checkpointId, long checkpointTimestamp) {
    return elementsGenerated;
  }

  @Override
  public void restoreState(Long state) {
    elementsGenerated = state;
  }

}
