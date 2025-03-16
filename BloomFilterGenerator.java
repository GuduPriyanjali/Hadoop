import java.io.*;
import org.apache.hadoop.util.bloom.*;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.DistributedCache;

public class BloomFilterGenerator {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Bloom Filter parameters
        int numElements = 100000; // Expected number of elements
        float falsePositiveRate = 0.01f; // False positive rate
        int vectorSize = BloomFilter.optimalSize(numElements, falsePositiveRate);
        int numHashFunctions = BloomFilter.optimalNumOfHashFunctions(numElements, vectorSize);

        BloomFilter filter = new BloomFilter(vectorSize, numHashFunctions, Hash.MURMUR_HASH);

        // Read dataset B and add keys to the Bloom Filter
        BufferedReader reader = new BufferedReader(new FileReader("datasetB.txt"));
        String line;
        while ((line = reader.readLine()) != null) {
            filter.add(new Key(line.getBytes()));
        }
        reader.close();

        // Write Bloom Filter to HDFS
        FSDataOutputStream out = fs.create(new Path("bloomfilter"));
        filter.write(out);
        out.close();
  }
}
