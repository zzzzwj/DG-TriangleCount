import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class DGDriver {

    public static void main(String[] args) throws Exception{
        if(args.length != 3){
            System.out.println("Usage: hadoop jar *.jar input output num_reducer");
            System.exit(-1);
        }

        String[] AdjTable_param = {args[0], "t_AdjTable/", args[2]};
        AdjTable.main(AdjTable_param);
        String[] TriangleCount_param = {AdjTable_param[1], "t_TriangleCount/", args[2]};
        TriangleCount.main(TriangleCount_param);
        String[] CountSum_param = {TriangleCount_param[1], args[1], args[2]};
        CountSum.main(CountSum_param);

        // clear the buffer directories
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(AdjTable_param[1]),true);
        fs.delete(new Path(TriangleCount_param[1]),true);

        FSDataInputStream in = fs.open(new Path(args[1] + "/part-r-00000"));
        IOUtils.copyBytes(in, System.out, 1024);
        IOUtils.closeStream(in);
    }
}
