/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 16-3-6
 * Time: 上午10:56
 * To change this template use File | Settings | File Templates.
 */
//package java.io;
import com.jmatio.io.MatFileReader;
import com.jmatio.types.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
public class mat2Seq {
    public static void main(String[] args) throws IOException {
        //writeMat2Seq("data/1k_1k/F1k1k.mat","SeqOutput/F1k1k");
        writeMat2Seq("data/1k_5k/b1k5k.mat","SeqOutput/b1k5k");
        //writeMat2Seq("data/100_100/b100.mat","SeqOutput/b100");
        //writeMat2Seq("data/1k1100/mat1k1100.mat","SeqOutput/test1k1100");
        //writeMat2Seq("data/B1k2w.mat","SeqOutput/1k2w");

        //writeMat2Seq("data/1k_2w/B1k2w.mat","SeqOutput5/B1k2w");
    }

    public static void writeMat2Seq(String matPath,String SeqOutput) throws IOException {
        MatFileReader reader=new MatFileReader(matPath);
        MLArray mlArray=reader.getMLArray("a");
        MLDouble doubleValue=(MLDouble)mlArray;
        double[][] matrix=doubleValue.getArray();
        Configuration conf =new Configuration();
        //FileSystem fs=FileSystem.get(URI.create(SeqOutput),conf);
        FileSystem fs=FileSystem.get(conf);
        Path path=new Path(SeqOutput);
        //FSDataOutputStream outputStream=fs.create(path);
        IntWritable key=new IntWritable();
        DoubleArrayWritable value=new DoubleArrayWritable();
        SequenceFile.Writer writer=null;
        try {
            writer=SequenceFile.createWriter(fs,conf,path,key.getClass(),value.getClass());

            // SequenceFile.Writer.Option
            if (matPath.endsWith("F1k.mat")){    //左矩阵F依次将行存储到Seq
                DoubleWritable[] rowVector=new DoubleWritable[matrix[0].length];
                for (int i=0;i<matrix.length;++i){
                    for (int j=0;j<matrix[0].length;++j){
                        rowVector[j]=new DoubleWritable(0);
                        rowVector[j].set(matrix[i][j]);
                    }
                    value.set(rowVector);
                    key.set(i);
                    writer.append(key,value);
                }
                writer.close();
                //outputStream.close();
                fs.close();
            }
            else{          //其他右矩阵依次将列存储到Seq中
                //DoubleWritable[] columnVector=new DoubleWritable[matrix[0].length];
                DoubleWritable[] columnVector=new DoubleWritable[matrix.length];
                for (int i=0;i<matrix[0].length;++i){
                    for (int j=0;j<matrix.length;++j){
                        columnVector[j]=new DoubleWritable(0);
                        columnVector[j].set(matrix[j][i]);
                    }
                    value.set(columnVector);
                    key.set(i);
                    writer.append(key,value);
                }
                writer.close();
                //outputStream.close();
                fs.close();

            }
        }
        finally {
        }
        System.out.println(matPath+"write done!");
    }
}
class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable(){
        super(DoubleWritable.class);
    }
    /*

    public String toString(){
        StringBuilder sb=new StringBuilder();
        for (Writable val:get()){
            DoubleWritable doubleWritable=(DoubleWritable)val;
            sb.append(doubleWritable.get());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
    */

}
