package joinexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class example {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int colIDLeft = 0;
        int colIDRight = 1;

        JavaRDD<String> rawInputLeft = sc.textFile("src/main/resources/"+"customer"+".csv");
        JavaRDD<String> rawInputRight = sc.textFile("src/main/resources/"+"payment"+".csv");

        JavaRDD<String> inputLeft = trim(rawInputLeft);
        System.out.println(inputLeft.collect());
        JavaRDD<String> inputRight = trim(rawInputRight);
        System.out.println(inputRight.collect());

        JavaPairRDD<String,String> pairsLeft = TupleSerializable.tupleHelper(inputLeft,colIDLeft);
        JavaPairRDD<String,String> pairsRight = TupleSerializable.tupleHelper(inputRight,colIDRight);


        JavaRDD<String> output = pairsLeft.join(pairsRight).values().map(x->x._1+","+x._2);
        System.out.println(output.collect());

    }

    public static JavaRDD<String> trim(JavaRDD<String> input){
        return input.map(x-> {
            String output2 = "";
            String[] cols = x.split(",");
            for(int a=0;a<4;a++){
                String colID = String.valueOf(a);
                if (output2.equals(""))output2=cols[Integer.parseInt(colID)];
                else output2=output2+","+cols[Integer.parseInt(colID)];
            }
            return output2;
        });
    }
}
