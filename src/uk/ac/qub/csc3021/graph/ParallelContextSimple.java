package uk.ac.qub.csc3021.graph;

public class ParallelContextSimple extends ParallelContext {
    // references to the objects needed as parameters in ranged_edgemap
    SparseMatrix matrixRef;
    Relax relaxRef;

    private class ThreadSimple extends Thread {
        private int from;
        private int to;

        ThreadSimple(int from, int to) {
            this.from = from;
            this.to = to;
        }

        public void run() {
            matrixRef.ranged_edgemap(relaxRef, from, to);
        }
    }

    public ParallelContextSimple(int num_threads_) {
        super(num_threads_);
    }

    public void terminate() {
    }

    // The edgemap method for Q3 should create threads, which each process
    // one graph partition, then wait for them to complete.
    public void edgemap(SparseMatrix matrix, Relax relax) {
        matrixRef = matrix;
        relaxRef = relax;

        double numVertices = matrix.getNumVertices();
        int numThreads = getNumThreads();
        int rangeLength = (int) Math.ceil(numVertices / numThreads);

        ThreadSimple[] threads = new ThreadSimple[numThreads];

        // create the threads and pass in the ranges
        int from;
        int to;
        for (int i = 0; i < numThreads - 1; i++) {
            from = i * rangeLength;
            to = from + rangeLength - 1;
            threads[i] = new ThreadSimple(from, to);
        }
        // last partition
        from = (numThreads - 1) * rangeLength;
        to = (int) numVertices - 1;
        threads[numThreads - 1] = new ThreadSimple(from, to);

        // start all the threads
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }

        // wait for all the threads to complete by joining them
        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {

            }
        }
    }
}
