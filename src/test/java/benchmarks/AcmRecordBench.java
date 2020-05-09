package benchmarks;

import com.threadcat.acm.AcmRecord;
import com.threadcat.acm.TestColumns;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;

@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@Measurement(iterations = 1)
@Warmup(iterations = 1)
public class AcmRecordBench {

    @State(Scope.Benchmark)
    public static class ExecutionPlan {
        AcmRecord record;
        TestColumns columns;
        Random random;

        @Setup(Level.Invocation)
        public void onSetup() {
            random = new Random();
            record = new AcmRecord();
            columns = new TestColumns(record);
            record.setByteBuffer(ByteBuffer.allocate(record.size()));
            //record.setByteBuffer(memoryMappedFile(record.size()));
        }

        private static MappedByteBuffer memoryMappedFile(long size) throws IOException {
            //File dir = new File("/media/ocz");
            //File dir = new File("/media/adata");
            //File file = File.createTempFile("acm_record", "test", dir);
            File file = File.createTempFile("acm_record", "test");
            file.deleteOnExit();
            FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            return channel
                    .map(FileChannel.MapMode.READ_WRITE, 0, size)
                    .load();
        }
    }

    @Benchmark
    public void testRandomUpdates(ExecutionPlan plan) {
        plan.record.update(plan.columns.priceLast, plan.random.nextDouble());
        plan.record.update(plan.columns.quantityLast, plan.random.nextDouble());
        plan.record.commit();
    }
}
