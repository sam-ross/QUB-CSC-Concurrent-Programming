package uk.ac.qub.csc3021.graph;

import java.util.concurrent.atomic.AtomicIntegerArray;

// Calculate the connected components using disjoint set data structure
// This algorithm only works correctly for undirected graphs
public class DisjointSetCC {
    private static class DSCCRelax implements Relax {
        private AtomicIntegerArray parent;

        DSCCRelax(AtomicIntegerArray parent_) {
            this.parent = parent_;
        }

        public void relax(int src, int dst) {
            union(src, dst);
        }

        // returns the index of the root
        public int find(int x) {
            int u = x;
            while (true) {
                int v = parent.get(u);
                int w = parent.get(v);
                if (v == w) {   // found root node so return it
                    return v;
                } else {
                    parent.compareAndSet(u, v, w);  // if the parent of u is still equal to v then set it to w
                    u = v;
                }
            }

        }

        private void union(int x, int y) {
            int u = x;
            int v = y;
            while (true) {
                u = find(u);
                v = find(v);
                if (u == v) {
                    return;
                } else if (u < v) {
                    if (parent.compareAndSet(u, u, v)) { // checks that the parent of u is u to make sure that u is still the root
                        return;
                    }
                } else if (parent.compareAndSet(v, v, u)) {
                    return;
                }
            }
        }
    }

    public static int[] compute(SparseMatrix matrix) {
        long tm_start = System.nanoTime();

        final int n = matrix.getNumVertices();
        final AtomicIntegerArray parent = new AtomicIntegerArray(n);
        final boolean verbose = true;

        // makeSet
        for (int i = 0; i < n; ++i) {
            // Each vertex is a set on their own
            // does the equivalent of makeSet for every vertex - randomized index set here
            // every vertex has a self-referencing pointer to itself as it is the parent and root of its disjoint set
            parent.set(i, i);
        }

        DSCCRelax DSCCrelax = new DSCCRelax(parent);

        double tm_init = (double) (System.nanoTime() - tm_start) * 1e-9;
        System.err.println("Initialisation: " + tm_init + " seconds");
        tm_start = System.nanoTime();

        ParallelContext context = ParallelContextHolder.get();

        // 1. Make pass over graph
        context.edgemap(matrix, DSCCrelax);

        double tm_step = (double) (System.nanoTime() - tm_start) * 1e-9;
        if (verbose)
            System.err.println("processing time=" + tm_step + " seconds");

        // Post-process the labels

        // 1. Count number of components
        //    and map component IDs to narrow domain
        int ncc = 0;
        int remap[] = new int[n];
        for (int i = 0; i < n; ++i)
            if (DSCCrelax.find(i) == i)
                remap[i] = ncc++;   // set the roots equal to the current count of the connected components?

        if (verbose)
            System.err.println("Number of components: " + ncc);

        // 2. Calculate size of each component
        int sizes[] = new int[ncc];
        for (int i = 0; i < n; ++i)
            ++sizes[remap[DSCCrelax.find(i)]];

        if (verbose)
            System.err.println("DisjointSetCC: " + ncc + " components");

        return sizes;
    }
}
