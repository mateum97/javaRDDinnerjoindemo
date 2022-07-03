# javaRDDinnerjoindemo
Simple code example how to do join on 2 spark rdds in java (without getting serializing errors). Equivalent to the SQL inner join.


What it does it takes 2 String JavaRDDs, creates a JavaPairRDD for each of them, each with the join column content as key. It does that in an external class in order to avoid the Spark Serialization error. The output is again String JavaRDD with the number of columns equal to the sum of the number of columns of the inputs.

Bear in mind that this example uses CSV input for JavaRDD, if another format is to be used then the trim method and the tupleHelper need to be adjusted