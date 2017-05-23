package com.streever.hadoop.hive.ql.io;

        import java.io.DataOutputStream;
        import java.io.IOException;
        import org.apache.hadoop.fs.FSDataOutputStream;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapred.JobConf;
        import org.apache.hadoop.mapred.RecordWriter;
        import org.apache.hadoop.mapred.Reporter;
        import org.apache.hadoop.mapred.TextOutputFormat;
//        import org.apache.hadoop.mapred.TextOutputFormat.LineRecordWriter;
        import org.apache.hadoop.util.Progressable;

public class HiveTeraOutputFormat extends TextOutputFormat<Text, Text> {
    static final String FINAL_SYNC_ATTRIBUTE = "terasort.final.sync";

    public HiveTeraOutputFormat() {
    }

    public static void setFinalSync(JobConf conf, boolean newValue) {
        conf.setBoolean("terasort.final.sync", newValue);
    }

    public static boolean getFinalSync(JobConf conf) {
        return conf.getBoolean("terasort.final.sync", false);
    }

    public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        Path dir = getWorkOutputPath(job);
        FileSystem fs = dir.getFileSystem(job);
        FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
        return new HiveTeraOutputFormat.TeraRecordWriter(fileOut, job);
    }

    static class TeraRecordWriter extends LineRecordWriter<Text, Text> {
        private static final byte[] newLine = "\r\n".getBytes();
        private boolean finalSync = false;

        public TeraRecordWriter(DataOutputStream out, JobConf conf) {
            super(out);
            this.finalSync = HiveTeraOutputFormat.getFinalSync(conf);
        }

        public synchronized void write(Text key, Text value) throws IOException {
            this.out.write(key.getBytes(), 0, key.getLength());
            this.out.write(value.getBytes(), 0, value.getLength());
            this.out.write(newLine, 0, newLine.length);
        }

        public void close() throws IOException {
            if(this.finalSync) {
                ((FSDataOutputStream)this.out).sync();
            }

            super.close((Reporter)null);
        }
    }
}
