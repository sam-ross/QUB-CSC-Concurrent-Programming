package uk.ac.qub.csc3021.graph;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

// This class represents the adjacency matrix of a graph as a sparse matrix
// in compressed sparse rows format (CSR), where a row index corresponds to
// a source vertex and a column index corresponds to a destination
public class SparseMatrixCSRCompetition extends SparseMatrix {
    // TODO: variable declarations
    private String source;
    private int num_threads;

    private int num_vertices; // Number of vertices in the graph
    private int num_edges;    // Number of edges in the graph
    private int[] startingPos;

//    // default constructor for validator
//    public SparseMatrixCSRCompetition(String source) {
////        System.out.println("Default constructor called");
//        this.source = source;
//        this.num_threads = 1;
//        try {
//            InputStreamReader is = new InputStreamReader(new FileInputStream(source), "UTF-8");
//            BufferedReader rd = new BufferedReader(is);
//
//            String line = rd.readLine();
//            if (line == null)
//                throw new Exception("premature end of file");
//            if (!line.equalsIgnoreCase("CSR") && !line.equalsIgnoreCase("CSC-CSR"))
//                throw new Exception("file format error -- header");
//
//            num_vertices = getNext(rd);
//            num_edges = getNext(rd);
//            rd.close();
//        } catch (Exception e) {
//            System.err.println("Exception: " + e);
//            return;
//        }
//
//    }

    // overloaded constructor for when using multiple threads
    public SparseMatrixCSRCompetition(String source, int num_threads) {
//        System.out.println("Overloaded constructor called");
        this.source = source;
        this.num_threads = num_threads;

        // creating a separate file channel here then closing it before creating the other file channel for the buffers
        // for the individual threads, to ensure that the buffer memory gets released. The memory never seemed to
        // get released for this big buffer and hence caused a lot of server out of memory exceptions when running on
        // the server - as the memory is limited and is near its limit with the individual thread buffers already.
        try (RandomAccessFile file = new RandomAccessFile(source, "r")) {
            FileChannel fileChannel = file.getChannel();
            int fileChannelSize = (int) fileChannel.size();

            // start positions
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannelSize);
            byte prev;
            int newLines = 3;
            int vertices = 0;
            int edges = 0;
            while (newLines > 0) {
                prev = buffer.get();
                if (prev == 10) {
                    newLines--;
                } else if (newLines == 2) {
                    vertices = (vertices * 10) + prev - 48;
                } else if (newLines == 1) {
                    edges = (edges * 10) + prev - 48;
                }
            }
            this.num_vertices = vertices;
            this.num_edges = edges;

            // number of threads
            startingPos = new int[num_threads + 1];
            startingPos[0] = buffer.position(); // first position will always be after the edge's newline character

            int fileChannelSizeNew = fileChannelSize - startingPos[0];
            int bufferSize = fileChannelSizeNew / num_threads;

            // get all the start and end positions for the buffers
            for (int i = 1; i < num_threads; i++) {
                int start = startingPos[i - 1] + bufferSize;
                if (start >= fileChannelSize) {
                    start = fileChannelSize - 1;
                }
                buffer.position(start);

                byte prev2 = buffer.get();
                while (prev2 != 10 && buffer.remaining() > 0) {
                    prev2 = buffer.get();
                }

                startingPos[i] = buffer.position();
            }
            startingPos[num_threads] = fileChannelSize;

            fileChannel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    int getNext(BufferedReader rd) throws Exception {
        String line = rd.readLine();
        if (line == null)
            throw new Exception("premature end of file");
        return Integer.parseInt(line);
    }

    // Return number of vertices in the graph
    public int getNumVertices() {
        return num_vertices;
    }

    // Return number of edges in the graph
    public int getNumEdges() {
        return num_edges;
    }

    // Auxiliary function for PageRank calculation
    public void calculateOutDegree(int outdeg[]) {
        // TODO:
        //    Calculate the out-degree for every vertex, i.e., the
        //    number of edges where a vertex appears as a source vertex.
//        for (int i = 0; i < num_vertices; i++) {
//            outdeg[i] = index[i + 1] - index[i];
//        }
    }

    // Apply relax once to every edge in the graph
    public void edgemap(Relax relax) {
        try (RandomAccessFile file = new RandomAccessFile(source, "r")) {
            FileChannel fileChannel = file.getChannel();
            int fileChannelSize = (int) fileChannel.size();

            // number of threads
            int fileChannelSizeNew = fileChannelSize - startingPos[0];
            int bufferSize = fileChannelSizeNew / num_threads;

            ThreadSimple[] threads = new ThreadSimple[num_threads];

            // create all the buffers
            System.out.println("bufferSize: " + bufferSize);
            for (int i = 0; i < num_threads; i++) {
                threads[i] = new ThreadSimple(
                        // the +1 for the size ensures that the last value of the buffer is a new line character
                        fileChannel.map(
                                FileChannel.MapMode.READ_ONLY,
                                startingPos[i],
                                startingPos[i + 1] - startingPos[i]
                        ),
                        relax
                );
                threads[i].start();
            }

            for (int i = 0; i < num_threads; i++) {
                threads[i].join();
            }

            fileChannel.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void ranged_edgemap(Relax relax, int from, int to) {
        // Only implement for parallel/concurrent processing
        // if you find it useful
//        for (int i = from; i <= to; i++) {
//            for (int j = index[i]; j < index[i + 1]; j++) {
//                relax.relax(i, destinations[j]);
//            }
//        }
    }
}

class ThreadSimple extends Thread {
    private MappedByteBuffer buffer;
    private Relax relax;

    ThreadSimple(MappedByteBuffer buffer, Relax relax) {
        this.buffer = buffer;
        this.relax = relax;
    }

    public void run() {
        // variable declarations
        char ch;
        int source;
        int destination;
        StringBuilder stringBuilder = new StringBuilder();

        // only needs to check at every new line as that's when each buffer ends
        while (buffer.remaining() > 0) {
            ch = (char) buffer.get();

            // get source value
            while (ch != ' ' && ch != '\n') {
                stringBuilder.append(ch);
                ch = (char) buffer.get();
            }

            source = Integer.parseInt(stringBuilder.toString());
            stringBuilder.setLength(0); // resets the string builder to empty

            // get destination value
            while (ch != '\n') {
                ch = (char) buffer.get();
                while (ch != ' ' && ch != '\n') {
                    stringBuilder.append(ch);
                    ch = (char) buffer.get();
                }
                destination = Integer.parseInt(stringBuilder.toString());
                relax.relax(source, destination);
                stringBuilder.setLength(0);
            }

//
//            prev = buffer.get();
//            src = 0;
//            while (prev != 10 && prev != 32) {
//                src = (src * 10) + prev - 48;
//                prev = buffer.get();
//            }
//            while (prev != 10) {
//                prev = buffer.get();
//                dst = 0;
//                while (prev != 10 && prev != 32) {
//                    dst = (dst * 10) + prev - 48;
//                    prev = buffer.get();
//                }
//                relax.relax(src, dst);
//            }

        }
    }

}
