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

  private long elementsGenerated;

  public LoadGeneratorSource(int loadTargetHz, int timeSliceLengthMs, long totalEventsToGenerate) {
    this.loadTargetHz = loadTargetHz;
    this.timeSliceLengthMs = timeSliceLengthMs;
    this.totalEventsToGenerate = totalEventsToGenerate;
  }

  /**
   * Subclasses must override this to generate a data element
   */
  public abstract T generateElement();


  /**
   * The main loop
   */
  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    final Object checkpointLock = sourceContext.getCheckpointLock();

    int elements = loadPerTimeslice();
    long totalElementsPerTask = 
      totalEventsToGenerate / getRuntimeContext().getNumberOfParallelSubtasks();

    while (running && elementsGenerated < totalElementsPerTask) {
      synchronized (checkpointLock) {
				long emitStartTime = System.currentTimeMillis();
				for (int i = 0; i < elements; i++) {
					sourceContext.collect(generateElement());
				}
				// Sleep for the rest of timeslice if needed
				long emitTime = System.currentTimeMillis() - emitStartTime;
				if (emitTime < timeSliceLengthMs) {
					Thread.sleep(timeSliceLengthMs - emitTime);
				}
				elementsGenerated += elements;
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
