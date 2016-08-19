package nl.surfsara.hadoop.seti.mapreduce;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class VerifyMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger logger = Logger.getLogger(VerifyMapper.class);

    private static enum Counters {
        CURRENT_PATH, BYTES_WRITTEN, PERCENTAGE_COMPLETE
    }

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        // Values -> path on hdfs; localize
        String localDir = context.getConfiguration().get("job.local.dir") + "/" + UUID.randomUUID().toString() + "/";
        Path hdfsPath = new Path(value.toString());
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream hdfsIn = fs.open(hdfsPath);
        unTarGZStream(hdfsIn, new File(localDir));
        hdfsIn.close();

        // In localdir will now be a products dir with all logs and dats
        // Go over all dats, count all lines. Count hitnums
        List<String> results = new ArrayList<String>();
        File products = new File(localDir + "products");
        String[] datFiles = products.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".dat");
            }
        });
        for (String fName : datFiles) {
            Scanner scan = new Scanner(new File(products, fName)).useDelimiter("\n");
            int lineCount = 0;
            int recordCount = 0;

            Pattern p = Pattern.compile("^\\d{3,}.*");
            while (scan.hasNext()) {
                String line = scan.next();
                lineCount++;

                if (p.matcher(line).matches()) {
                    recordCount++;
                }
            }
            results.add(fName + ";" + lineCount + ";" + recordCount + ";" + (lineCount - recordCount));
        }
        for (String res : results) {
            context.write(value, new Text(res));
        }
        // Remove local scratch files
        File localDirs = new File(localDir);
        localDirs.delete();

    }

    public void unTarGZStream(InputStream is, File destinationDir) throws IOException {
        TarArchiveInputStream in = new TarArchiveInputStream(new GZIPInputStream(is));
        TarArchiveEntry entry = in.getNextTarEntry();
        while (entry != null) {
            if (entry.isDirectory()) {
                entry = in.getNextTarEntry();
                continue;
            }
            File curfile = new File(destinationDir, entry.getName());
            File parent = curfile.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            OutputStream out = new FileOutputStream(curfile);
            IOUtils.copyLarge(in, out);
            out.close();
            entry = in.getNextTarEntry();
        }
        in.close();
    }


}
