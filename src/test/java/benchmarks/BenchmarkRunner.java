package benchmarks;

import org.openjdk.jmh.annotations.*;

public class BenchmarkRunner {

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        @Param({ "100", "200" })
        public int iterations;

        @Setup(Level.Invocation)
        public void setUp() {
            //
        }
    }
}
