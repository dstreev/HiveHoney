package com.streever.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.StringUtils;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HiveTeraInputFormat extends FileInputFormat<Text, Text> {
    static final String PARTITION_FILENAME = "_partition.lst";
    static final String SAMPLE_SIZE = "terasort.partitions.sample";
    private static JobConf lastConf = null;
    private static InputSplit[] lastResult = null;

    public HiveTeraInputFormat() {
    }

    public static void writePartitionFile(JobConf conf, Path partFile) throws IOException {
        HiveTeraInputFormat inFormat = new HiveTeraInputFormat();
        HiveTeraInputFormat.TextSampler sampler = new HiveTeraInputFormat.TextSampler();
        Text key = new Text();
        Text value = new Text();
        int partitions = conf.getNumReduceTasks();
        long sampleSize = conf.getLong("terasort.partitions.sample", 100000L);
        InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
        int samples = Math.min(10, splits.length);
        long recordsPerSample = sampleSize / (long)samples;
        int sampleStep = splits.length / samples;
        long records = 0L;

        for(int outFs = 0; outFs < samples; ++outFs) {
            RecordReader writer = inFormat.getRecordReader(splits[sampleStep * outFs], conf, (Reporter)null);

            while(writer.next(key, value)) {
                sampler.addKey(key);
                ++records;
                if((long)(outFs + 1) * recordsPerSample <= records) {
                    break;
                }
            }
        }

        FileSystem var23 = partFile.getFileSystem(conf);
        if(var23.exists(partFile)) {
            var23.delete(partFile, false);
        }

        SequenceFile.Writer var24 = SequenceFile.createWriter(var23, conf, partFile, Text.class, NullWritable.class);
        NullWritable nullValue = NullWritable.get();
        Text[] arr$ = sampler.createPartitions(partitions);
        int len$ = arr$.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            Text split = arr$[i$];
            var24.append(split, nullValue);
        }

        var24.close();
    }

    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new HiveTeraInputFormat.TeraRecordReader(job, (FileSplit)split);
    }

    public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
        if(conf == lastConf) {
            return lastResult;
        } else {
            lastConf = conf;
            lastResult = super.getSplits(conf, splits);
            return lastResult;
        }
    }

    static class TeraRecordReader implements RecordReader<Text, Text> {
        private LineRecordReader in;
        private LongWritable junk = new LongWritable();
        private Text line = new Text();
        private static int KEY_LENGTH = 10;

        public TeraRecordReader(Configuration job, FileSplit split) throws IOException {
            this.in = new LineRecordReader(job, split);
        }

        public void close() throws IOException {
            this.in.close();
        }

        public Text createKey() {
            return new Text();
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return this.in.getPos();
        }

        public float getProgress() throws IOException {
            return this.in.getProgress();
        }

        public boolean next(Text key, Text value) throws IOException {
            if(this.in.next(this.junk, this.line)) {
                if(this.line.getLength() < KEY_LENGTH) {
                    key.set(this.line);
                    value.clear();
                } else {
                    byte[] bytes = this.line.getBytes();
                    key.set(bytes, 0, KEY_LENGTH);
                    value.set(bytes, KEY_LENGTH, this.line.getLength() - KEY_LENGTH);
                }

                return true;
            } else {
                return false;
            }
        }
    }

    static class TextSampler implements IndexedSortable {
        private ArrayList<Text> records = new ArrayList();

        TextSampler() {
        }

        public int compare(int i, int j) {
            Text left = (Text)this.records.get(i);
            Text right = (Text)this.records.get(j);
            return left.compareTo(right);
        }

        public void swap(int i, int j) {
            Text left = (Text)this.records.get(i);
            Text right = (Text)this.records.get(j);
            this.records.set(j, left);
            this.records.set(i, right);
        }

        public void addKey(Text key) {
            this.records.add(new Text(key));
        }

        Text[] createPartitions(int numPartitions) {
            int numRecords = this.records.size();
            System.out.println("Making " + numPartitions + " from " + numRecords + " records");
            if(numPartitions > numRecords) {
                throw new IllegalArgumentException("Requested more partitions than input keys (" + numPartitions + " > " + numRecords + ")");
            } else {
                (new QuickSort()).sort(this, 0, this.records.size());
                float stepSize = (float)numRecords / (float)numPartitions;
                System.out.println("Step size is " + stepSize);
                Text[] result = new Text[numPartitions - 1];

                for(int i = 1; i < numPartitions; ++i) {
                    result[i - 1] = (Text)this.records.get(Math.round(stepSize * (float)i));
                }

                return result;
            }
        }
    }
}
