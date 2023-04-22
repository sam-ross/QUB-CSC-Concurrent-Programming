package uk.ac.qub.csc3021.graph;

public class ParallelContextSimpleCompetition extends ParallelContext {
    public ParallelContextSimpleCompetition(int num_threads_) {
        super(num_threads_);
    }

    public void terminate() {
    }

    // The edgemap method for Q3 should create threads, which each process
    // one graph partition, then wait for them to complete.
    public void edgemap(SparseMatrix matrix, Relax relax) {
        matrix.edgemap(relax);
    }
}
